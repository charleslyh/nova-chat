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
    response_object, AgentId, AppendEvent, Attempt, ChainLimits, ClaimedResponse,
    ConversationError, ConversationStore, LedgerError, RequestProvenance, ResolvedContext,
    ResponseEventKind, ResponseEventLog, ResponseId, ResponseItem, ResponseLedger, ResponseRecord,
    ResponseStatus, SnapshotRef, StoreError, TenantId, TurnCommit, Usage,
};
use serde_json::Value;
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::runner::{AgentError, AgentRunner, AgentTask};
use crate::sink::EventSink;

/// What a finished run produced, bundled so the terminal funnels take one
/// value instead of four parallel parameters (the fields are one fact: the
/// outcome of this attempt).
struct TerminalOutput {
    produced: Vec<ResponseItem>,
    reasoning: String,
    usage: Usage,
    status: ResponseStatus,
}

/// Ports the orchestrator needs.
pub struct AgentRuntimeDeps {
    pub ledger: Arc<dyn ResponseLedger>,
    pub event_log: Arc<dyn ResponseEventLog>,
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
            Err(LedgerError::Store(StoreError::ReadOnly)) => {
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
                response_object(&record, &[]),
            ))
            .await;

        // History lives in the conversation snapshot (D30): read it once from the
        // anchor rather than walking a chain, then append this turn's own input.
        let snapshot = match self.resolve_snapshot(&record).await {
            Ok(ctx) => ctx,
            Err(e) => {
                warn!(response = %id, error = %e, "could not resolve context snapshot");
                return self.fail(&record, attempt, Usage::default(), &e.to_string(), now_ms).await;
            }
        };
        let mut items: Vec<ResponseItem> = snapshot.items;
        items.extend(record.input_items.clone());

        let task = AgentTask {
            model: record.model.clone(),
            instructions: record.instructions.clone(),
            tools: record.tools.clone(),
            tool_choice: record.tool_choice.clone(),
            items,
            provenance: RequestProvenance {
                response_id: id.clone(),
                attempt,
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
            TerminalOutput {
                produced: outcome.items,
                reasoning: sink.reasoning().to_string(),
                usage: outcome.usage,
                status: outcome.status,
            },
            now_ms,
        )
        .await
    }

    /// Terminal funnel for a response that produced output.
    async fn complete(
        &self,
        record: &ResponseRecord,
        attempt: Attempt,
        output: TerminalOutput,
        now_ms: u64,
    ) -> Executed {
        let TerminalOutput {
            produced,
            reasoning,
            usage,
            status,
        } = output;
        let id = &record.response_id;

        if let Err(e) = self
            .deps
            .ledger
            .complete(id, attempt, status, usage, now_ms)
            .await
        {
            warn!(response = %id, error = %e, "complete failed");
            return Executed::Failed;
        }

        // Durable output goes to the conversation snapshot (D30), sourced from the
        // runner's final items — never from replaying the event stream (INV-48).
        //
        // A conversation-anchored response must persist its turn to the snapshot;
        // a bare response (no conversation) has no snapshot to append to, but it is
        // still "stored": its record and the terminal event (carrying the full
        // object, output included) are its retention under D30.
        let output_stored = if record.stored {
            if record.conversation_id.is_none() {
                true
            } else {
                let reasoning = if reasoning.is_empty() {
                    None
                } else {
                    Some(reasoning.clone())
                };
                self.append_turn(record, &produced, reasoning, usage, status, now_ms)
                    .await
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
        // Re-read the record: `ledger.complete` has written the terminal status
        // and usage, and the terminal event must carry them (the claimed record
        // still says `in_progress`).
        let updated = self
            .deps
            .ledger
            .get(id)
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| record.clone());
        let value = response_object(&updated, &produced);
        self.close_stream(id, kind, value, now_ms).await;
        info!(response = %id, "completed");
        Executed::Completed
    }

    /// Terminal funnel for a response that produced nothing usable.
    async fn fail(
        &self,
        record: &ResponseRecord,
        attempt: Attempt,
        usage: Usage,
        reason: &str,
        now_ms: u64,
    ) -> Executed {
        let id = &record.response_id;

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
        // Re-read for the same reason as the completion path: `ledger.complete`
        // holds the terminal status the failure event must report.
        let updated = self
            .deps
            .ledger
            .get(id)
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| record.clone());
        let value = response_object(&updated, &[]);
        self.close_stream(id, ResponseEventKind::Failed, value, now_ms)
            .await;
        Executed::Failed
    }

    /// Resolve the context a claim inherits, from the record's anchor (D30).
    ///
    /// `Previous` bare chains are mapped through the ledger to their conversation,
    /// then read here. `Root` yields empty. A bare chain with no conversation has
    /// no durable snapshot and is reported as broken.
    async fn resolve_snapshot(&self, record: &ResponseRecord) -> Result<ResolvedContext, ConversationError> {
        let tenant = &record.tenant_id;
        let anchor = record.anchor();
        let resolved = match &anchor {
            SnapshotRef::Root => ResolvedContext::default(),
            SnapshotRef::Conversation(id) => self.read_snapshot(tenant, id).await?,
            SnapshotRef::Previous(id) => {
                let prev = self
                    .deps
                    .ledger
                    .get(id)
                    .await
                    .map_err(|_| ConversationError::ChainBroken(id.clone()))?
                    .ok_or_else(|| ConversationError::ChainBroken(id.clone()))?;
                match prev.anchor() {
                    SnapshotRef::Conversation(cid) => self.read_snapshot(tenant, &cid).await?,
                    // Bare response: reconstruct its own input+output from the stream.
                    SnapshotRef::Root => self.reconstruct_bare(&prev).await?,
                    SnapshotRef::Previous(_) => {
                        return Err(ConversationError::ChainBroken(id.clone()));
                    }
                }
            }
        };
        Ok(resolved)
    }

    /// Reconstruct a bare (conversation-less) response's input+output from the
    /// event stream: input from the record, output from the terminal event.
    async fn reconstruct_bare(
        &self,
        record: &ResponseRecord,
    ) -> Result<ResolvedContext, ConversationError> {
        let mut items = record.input_items.clone();
        let mut bytes = items.iter().map(ResponseItem::byte_len).sum::<usize>();
        let mut cursor: Option<u64> = None;
        loop {
            let batch = self
                .deps
                .event_log
                .read_after(&record.response_id, cursor, 256, 0)
                .await
                .map_err(|e| ConversationError::Store(StoreError::Internal(e.to_string())))?;
            if batch.is_empty() {
                break;
            }
            let terminal = batch.iter().any(|e| e.kind.is_terminal());
            cursor = batch.last().map(|e| e.sequence_number);
            for event in &batch {
                if let nova_responses::EventBody::Response { response } = &event.body {
                    if let Ok(output) = serde_json::from_value::<Vec<ResponseItem>>(
                        response.get("output").cloned().unwrap_or_default(),
                    ) {
                        bytes += output.iter().map(ResponseItem::byte_len).sum::<usize>();
                        items.extend(output);
                    }
                }
            }
            if terminal {
                break;
            }
        }
        Ok(ResolvedContext {
            items,
            reasoning: Vec::new(),
            depth: 1,
            bytes,
        })
    }

    /// Read a conversation snapshot, returning `Unavailable` when no conversation
    /// port is mounted (the runtime can then fail the turn explicitly).
    async fn read_snapshot(
        &self,
        tenant: &TenantId,
        id: &nova_responses::ConversationId,
    ) -> Result<ResolvedContext, ConversationError> {
        match &self.deps.conversations {
            Some(store) => store.read_snapshot(tenant, id).await,
            None => Err(ConversationError::Store(StoreError::Unavailable)),
        }
    }

    /// Append this turn's output to its conversation snapshot. Returns `true` when
    /// durably stored; `false` when there is no conversation to store into (a bare
    /// response has no durable home, so `stored` cannot be honoured).
    async fn append_turn(
        &self,
        record: &ResponseRecord,
        produced: &[ResponseItem],
        reasoning: Option<String>,
        usage: Usage,
        status: ResponseStatus,
        now_ms: u64,
    ) -> bool {
        let (Some(store), Some(conversation_id)) =
            (&self.deps.conversations, &record.conversation_id)
        else {
            return false;
        };
        match store
            .append_turn(
                &record.tenant_id,
                conversation_id,
                &record.response_id,
                TurnCommit {
                    input_items: record.input_items.clone(),
                    output_items: produced.to_vec(),
                    reasoning,
                    usage,
                    status,
                },
                now_ms,
            )
            .await
        {
            Ok(_) => true,
            Err(e) => {
                warn!(
                    response = %record.response_id,
                    conversation = %conversation_id,
                    error = %e,
                    "storing output to the conversation snapshot failed"
                );
                false
            }
        }
    }

    /// Release the turn lock, and advance the conversation tail when asked.
    async fn settle(
        &self,
        record: &ResponseRecord,
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

    /// Emit the terminal event (carrying the full response object, output included)
    /// and start the retention window.
    async fn close_stream(
        &self,
        id: &ResponseId,
        kind: ResponseEventKind,
        value: Value,
        now_ms: u64,
    ) {
        let _ = self
            .deps
            .event_log
            .append(AppendEvent::lifecycle(id.clone(), kind, value))
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
                Err(LedgerError::Store(StoreError::ReadOnly)) => break,
                Err(e) => warn!(error = %e, "heartbeat failed"),
            }
        }
    });
    HeartbeatGuard { handle }
}
