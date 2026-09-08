//! In-process orchestrator: claim a response, run it via an [`AgentRunner`], and
//! commit the result.
//!
//! # Why the ReAct loop is not here
//!
//! The orchestrator knows *when* to claim, *how* to assemble the task, and *where*
//! to put the result. It deliberately does **not** know *how* a task is executed
//! — that is the [`AgentRunner`] implementation's business, and it is what lets a
//! mock provider and a real agent SDK sit behind the same seam. Everything the
//! runner produces flows back through the [`crate::EventSink`] (increments) and
//! the returned [`crate::AgentOutcome`] (final items).

use std::sync::Arc;
use std::time::Duration;

use nova_responses::{
    AgentId, AppendEvent, Attempt, ChainLimits, ClaimedResponse, ContextStore,
    ConversationStore, LedgerError, RequestProvenance, ResponseEventKind, ResponseEventLog,
    ResponseId, ResponseItem, ResponseLedger, ResponseStatus, StoredResponse, TenantId, Usage,
};
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::runner::{AgentError, AgentRunner, AgentTask};
use crate::sink::EventSink;

/// Ports the orchestrator needs.
pub struct AgentRuntimeDeps {
    pub ledger: Arc<dyn ResponseLedger>,
    pub event_log: Arc<dyn ResponseEventLog>,
    pub context: Arc<dyn ContextStore>,
    /// The execution seam. The orchestrator never sees how a task runs.
    pub runner: Arc<dyn AgentRunner>,
    /// Timestamp source: `SystemTime` in production, a virtual clock the test
    /// can advance (D15).
    pub now: Arc<dyn Fn() -> u64 + Send + Sync>,
    /// Conversation bookkeeping at terminal (D28). `None` when no conversation
    /// port is mounted.
    pub conversations: Option<Arc<dyn ConversationStore>>,
}

#[derive(Debug, Clone)]
pub struct AgentRuntimeConfig {
    pub exec_ttl_ms: u64,
    pub chain_limits: ChainLimits,
    pub retain_after_terminal_ms: u64,
    /// Hard ceiling on tool-calling rounds per response.
    pub max_tool_rounds: usize,
    /// Interval between keep-alive heartbeats sent while the (possibly long)
    /// ReAct loop runs.
    pub heartbeat_interval_ms: u64,
}

impl Default for AgentRuntimeConfig {
    fn default() -> Self {
        Self {
            exec_ttl_ms: 300_000,
            chain_limits: ChainLimits::default(),
            retain_after_terminal_ms: 60_000,
            max_tool_rounds: 20,
            heartbeat_interval_ms: 30_000,
        }
    }
}

/// What one execution attempt did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Executed {
    /// Nothing was queued for this node.
    Idle,
    Completed,
    /// Abandoned because the fence moved (reap or cancel). Not a failure.
    Superseded,
    /// Reported as failed.
    Failed,
}

/// The orchestrator.
pub struct AgentRuntime {
    deps: AgentRuntimeDeps,
    cfg: AgentRuntimeConfig,
}

impl AgentRuntime {
    pub fn new(deps: AgentRuntimeDeps, cfg: AgentRuntimeConfig) -> Self {
        Self { deps, cfg }
    }

    pub fn runner_name(&self) -> &str {
        self.deps.runner.name()
    }

    fn now_ms(&self) -> u64 {
        (self.deps.now)()
    }

    /// Take one queued response and run it to a terminal state.
    pub async fn run_once(&self, now_ms: u64) -> Executed {
        let agent = AgentId(uuid::Uuid::new_v4());

        let claimed = match self
            .deps
            .ledger
            .claim(agent, now_ms, self.cfg.exec_ttl_ms)
            .await
        {
            Ok(Some(c)) => c,
            Ok(None) => return Executed::Idle,
            Err(LedgerError::ReadOnly) => {
                debug!("ledger is read-only; not claiming");
                return Executed::Idle;
            }
            Err(e) => {
                warn!(error = %e, "claim failed");
                return Executed::Idle;
            }
        };

        self.serve(claimed, agent, now_ms).await
    }

    /// Drain everything currently queued.
    pub async fn drain(&self, now_ms: u64, max: usize) -> Vec<Executed> {
        let mut out = Vec::new();
        for _ in 0..max {
            let r = self.run_once(now_ms).await;
            let idle = r == Executed::Idle;
            out.push(r);
            if idle {
                break;
            }
        }
        out
    }

    /// Start the claim/poll loop as a background task, returning a handle whose
    /// [`AgentRuntimeHandle::stop`] stops it gracefully.
    pub fn start(self: &Arc<Self>, max_concurrent: usize, poll_interval_ms: u64) -> AgentRuntimeHandle {
        let (stop_tx, mut stop_rx) = watch::channel(None::<u64>);
        let runtime = self.clone();
        let join = tokio::spawn(async move {
            let permits = Arc::new(Semaphore::new(max_concurrent));
            let mut tasks = JoinSet::new();

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(poll_interval_ms)) => {
                        if let Ok(permit) = permits.clone().try_acquire_owned() {
                            let runtime = runtime.clone();
                            tasks.spawn(async move {
                                let _permit = permit;
                                let now = runtime.now_ms();
                                match runtime.run_once(now).await {
                                    Executed::Idle => {}
                                    Executed::Completed => info!("generation completed"),
                                    Executed::Superseded => {}
                                    Executed::Failed => info!("generation failed"),
                                }
                            });
                        }
                    }
                    _ = stop_rx.changed() => break,
                }
            }

            // Drain in-flight work, bounded by the stop budget. Anything still
            // running past the budget is abandoned (aborted on JoinSet drop) and
            // reclaimed by the next startup's orphan reclaim (INV-45).
            let budget_ms = *stop_rx.borrow();
            while !tasks.is_empty() {
                match budget_ms {
                    Some(ms) => {
                        let deadline = tokio::time::Instant::now() + Duration::from_millis(ms);
                        match tokio::time::timeout_at(deadline, tasks.join_next()).await {
                            Ok(Some(_)) => continue,
                            Ok(None) => break,
                            Err(_) => break,
                        }
                    }
                    None => {
                        if tasks.join_next().await.is_none() {
                            break;
                        }
                    }
                }
            }
        });
        AgentRuntimeHandle { stop_tx, join }
    }

    async fn serve(&self, claimed: ClaimedResponse, agent_id: AgentId, now_ms: u64) -> Executed {
        let record = claimed.record;
        let id = record.response_id.clone();
        let attempt = claimed.attempt;

        // Keep the claim alive across the whole (possibly long) run.
        let _heartbeat = spawn_heartbeat(
            self.deps.ledger.clone(),
            agent_id,
            self.deps.now.clone(),
            self.cfg.heartbeat_interval_ms,
        );

        // Announce the transition so a subscriber sees a defined progression.
        let _ = self
            .deps
            .event_log
            .append(AppendEvent::lifecycle_with_attempt(
                id.clone(),
                ResponseEventKind::InProgress,
                attempt,
                record.to_response_value(),
            ))
            .await;

        // History is materialised onto the record (D24): read the snapshot rather
        // than walking the chain.
        let mut items: Vec<ResponseItem> = record.context.clone();
        items.extend(record.input_items.clone());

        let task = AgentTask {
            model: record.model.clone(),
            instructions: record.instructions.clone(),
            tools: record.tools.clone(),
            tool_choice: record.tool_choice.clone(),
            items,
            provenance: RequestProvenance {
                response_id: id.to_string(),
                attempt: attempt.0,
                exec_deadline_ms: claimed.exec_deadline_ms,
            },
            max_tool_rounds: self.cfg.max_tool_rounds,
        };

        let mut sink = EventSink::new(self.deps.event_log.clone(), id.clone(), attempt);

        let outcome = match self.deps.runner.run(&task, &mut sink).await {
            Ok(outcome) => outcome,
            Err(AgentError::Superseded) => {
                info!(response = %id, "attempt superseded");
                return Executed::Superseded;
            }
            Err(AgentError::Failed { message, usage }) => {
                warn!(
                    response = %id,
                    runner = self.deps.runner.name(),
                    error = %message,
                    "agent run failed"
                );
                return self.fail(&record, attempt, usage, &message, now_ms).await;
            }
        };

        if sink.stopped() {
            info!(response = %id, "fence moved during generation; discarding output");
            return Executed::Superseded;
        }

        self.complete(
            &record,
            attempt,
            outcome.items,
            sink.reasoning().to_string(),
            outcome.usage,
            outcome.status,
            now_ms,
        )
        .await
    }

    /// Terminal funnel for a response that produced output.
    async fn complete(
        &self,
        record: &StoredResponse,
        attempt: Attempt,
        produced: Vec<ResponseItem>,
        reasoning: String,
        usage: Usage,
        status: ResponseStatus,
        now_ms: u64,
    ) -> Executed {
        let id = &record.response_id;
        let tenant = &record.tenant_id;

        if let Err(e) = self
            .deps
            .ledger
            .complete(id, attempt, status, usage, now_ms)
            .await
        {
            warn!(response = %id, error = %e, "complete failed");
            return Executed::Failed;
        }

        let output_stored = if record.stored {
            let reasoning = if reasoning.is_empty() {
                None
            } else {
                Some(reasoning)
            };
            match self
                .deps
                .context
                .append_output(tenant, id, produced, reasoning, usage, status, now_ms)
                .await
            {
                Ok(()) => true,
                Err(e) => {
                    warn!(response = %id, error = %e, "storing output failed");
                    false
                }
            }
        } else {
            false
        };

        self.settle(record, status, output_stored, now_ms).await;

        if record.stored && !output_stored {
            return Executed::Failed;
        }

        let kind = match status {
            ResponseStatus::Incomplete => ResponseEventKind::Incomplete,
            _ => ResponseEventKind::Completed,
        };
        self.close_stream(id, tenant, kind, now_ms).await;
        info!(response = %id, "completed");
        Executed::Completed
    }

    /// Terminal funnel for a response that produced nothing usable.
    async fn fail(
        &self,
        record: &StoredResponse,
        attempt: Attempt,
        usage: Usage,
        reason: &str,
        now_ms: u64,
    ) -> Executed {
        let id = &record.response_id;
        let tenant = &record.tenant_id;

        if let Err(e) = self
            .deps
            .ledger
            .complete(id, attempt, ResponseStatus::Failed, usage, now_ms)
            .await
        {
            warn!(response = %id, error = %e, "could not record failure");
            return Executed::Failed;
        }

        self.settle(record, ResponseStatus::Failed, false, now_ms)
            .await;

        debug!(response = %id, reason, "failed");
        self.close_stream(id, tenant, ResponseEventKind::Failed, now_ms)
            .await;
        Executed::Failed
    }

    /// Release the turn lock, and advance the conversation tail when asked.
    async fn settle(
        &self,
        record: &StoredResponse,
        status: ResponseStatus,
        advance_tail: bool,
        now_ms: u64,
    ) {
        let tenant = &record.tenant_id;
        let id = &record.response_id;

        if let (Some(conversations), Some(conversation_id)) =
            (&self.deps.conversations, &record.conversation_id)
        {
            if advance_tail {
                if let Err(e) = conversations.advance(tenant, conversation_id, id).await {
                    warn!(
                        response = %id,
                        conversation = %conversation_id,
                        error = %e,
                        "could not advance the conversation tail; the next turn will \
                         inherit the previous turn's context"
                    );
                }
            }
            if let Err(e) = conversations
                .release_active(tenant, conversation_id, id, status, now_ms)
                .await
            {
                warn!(
                    response = %id,
                    conversation = %conversation_id,
                    error = %e,
                    "could not release the turn marker; the conversation stays busy \
                     until a later terminal path releases it"
                );
            }
        }
    }

    /// Emit the terminal event and start the retention window.
    async fn close_stream(
        &self,
        id: &ResponseId,
        tenant: &TenantId,
        kind: ResponseEventKind,
        now_ms: u64,
    ) {
        let response = match self.deps.context.get(tenant, id).await {
            Ok(Some(record)) => record.to_response_value(),
            _ => serde_json::json!({
                "id": id.to_string(),
                "object": "response",
                "status": kind.as_str().strip_prefix("response.").unwrap_or(kind.as_str()),
            }),
        };
        let _ = self
            .deps
            .event_log
            .append(AppendEvent::lifecycle(id.clone(), kind, response))
            .await;
        let _ = self
            .deps
            .event_log
            .close(id, now_ms, self.cfg.retain_after_terminal_ms)
            .await;
    }
}

/// Handle returned by [`AgentRuntime::start`].
pub struct AgentRuntimeHandle {
    stop_tx: watch::Sender<Option<u64>>,
    join: tokio::task::JoinHandle<()>,
}

impl AgentRuntimeHandle {
    /// Stop gracefully: stop claiming new work, then wait for in-flight work to
    /// drain up to `drain_timeout_ms`. Returns when the loop has exited.
    pub async fn stop(self, drain_timeout_ms: u64) {
        let _ = self.stop_tx.send(Some(drain_timeout_ms));
        if let Err(e) = self.join.await {
            warn!(error = %e, "serve loop task failed");
        }
    }
}

/// Floor for the heartbeat interval, so a misconfigured `0` cannot turn the
/// background task into a hot spin.
const MIN_HEARTBEAT_INTERVAL_MS: u64 = 1_000;

/// RAII guard that aborts the background heartbeat task when dropped.
struct HeartbeatGuard {
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for HeartbeatGuard {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Spawn a background task that keeps the claim's heartbeat fresh while the
/// runner executes the (possibly long) ReAct loop.
fn spawn_heartbeat(
    ledger: Arc<dyn ResponseLedger>,
    agent_id: AgentId,
    now: Arc<dyn Fn() -> u64 + Send + Sync>,
    interval_ms: u64,
) -> HeartbeatGuard {
    let interval_ms = interval_ms.max(MIN_HEARTBEAT_INTERVAL_MS);
    let handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
            match ledger.heartbeat(agent_id, (now)()).await {
                Ok(()) => {}
                Err(LedgerError::ReadOnly) => break,
                Err(e) => warn!(error = %e, "heartbeat failed"),
            }
        }
    });
    HeartbeatGuard { handle }
}
