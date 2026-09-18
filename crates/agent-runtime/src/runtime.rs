//! In-process orchestrator: claim a response, run it via an [`AgentRunner`], and
//! commit the result.
//!
//! # Why the ReAct loop is not here
//!
//! The orchestrator knows *when* to claim, *how* to assemble the task, and *where* to
//! put the result. It deliberately does **not** know *how* a task is executed — that is
//! the [`AgentRunner`] implementation's business, and it is what lets a mock provider
//! and a real agent SDK sit behind the same seam. Everything the runner produces flows
//! back through the [`crate::EventSink`] (increments) and the returned
//! [`crate::AgentOutcome`] (final items).

use std::sync::Arc;
use std::time::Duration;

use nova_responses::ports::{
    ClaimedResponse, ConversationError, ConversationStore, LedgerError, ResponseClaimSource,
    ResponseEventLog, StoreError,
};
use nova_responses::protocol::ResponseObject;
use nova_responses::{AgentId, AppendEvent, Attempt, Clock, ContextAnchor, ConversationId, ResolvedContext, ResponseEventKind, ResponseId, ResponseItem, ResponseRecord, ResponseStatus, TenantId, TurnCommit, Usage};
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::runner::{AgentError, AgentRunner, AgentTask, CancelProbe};
use crate::sink::EventSink;

/// Events per read while replaying a bare response's stream.
const EVENT_REPLAY_PAGE: usize = 256;

/// Floor for the heartbeat interval, so a misconfigured zero cannot turn the
/// background task into a hot spin.
const MIN_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// Why the history a claim should run against could not be assembled.
///
/// Distinct from [`ConversationError`]: "this chain has no durable home" is the
/// execution side's own conclusion, not something a conversation store ever reports.
#[derive(Debug, thiserror::Error)]
enum SnapshotUnavailable {
    #[error("chain broken at {0}")]
    ChainBroken(ResponseId),
    #[error(transparent)]
    Conversation(#[from] ConversationError),
}

/// What a finished run produced, bundled so the terminal funnels take one value
/// instead of four parallel parameters (the fields are one fact: the outcome of this
/// attempt).
struct TerminalOutput {
    produced: Vec<ResponseItem>,
    reasoning: String,
    usage: Usage,
    status: ResponseStatus,
}

/// Ports the orchestrator needs.
pub struct AgentRuntimeDeps {
    pub ledger: Arc<dyn ResponseClaimSource>,
    pub event_log: Arc<dyn ResponseEventLog>,
    /// The execution seam. The orchestrator never sees how a task runs.
    pub runner: Arc<dyn AgentRunner>,
    /// Timestamp source: the system clock in production, a virtual one the test can
    /// advance (D15). The trait object is the seam: production mounts
    /// [`nova_responses::SystemClock`], verification mounts a clock that also has
    /// `advance`/`set`.
    pub clock: Arc<dyn Clock>,
    /// Conversation bookkeeping at terminal (D28). `None` when no conversation port is
    /// mounted.
    pub conversations: Option<Arc<dyn ConversationStore>>,
}

/// Execution-side knobs.
///
/// No chain limits here: they are enforced once, at admission, by the capability
/// layer. The copy that used to sit on this struct was never read — and had it been,
/// two configurable versions of one rule would eventually disagree about whether a
/// chain is acceptable, which is worse than having a single owner.
#[derive(Debug, Clone)]
pub struct AgentRuntimeConfig {
    pub exec_ttl: Duration,
    pub retain_after_terminal: Duration,
    /// Hard ceiling on tool-calling rounds per response.
    pub max_tool_rounds: usize,
    /// Interval between keep-alive heartbeats sent while the (possibly long) ReAct
    /// loop runs.
    pub heartbeat_interval: Duration,
    /// Interval between active cancellation polls while a blocking operation (notably a
    /// tool call) runs. Bounds the worst-case latency between a cancel/reap and the
    /// runner observing it and stopping its spend.
    pub cancel_poll_interval: Duration,
}

impl Default for AgentRuntimeConfig {
    fn default() -> Self {
        Self {
            exec_ttl: Duration::from_secs(300),
            retain_after_terminal: Duration::from_secs(60),
            max_tool_rounds: 20,
            heartbeat_interval: Duration::from_secs(30),
            cancel_poll_interval: Duration::from_millis(250),
        }
    }
}

/// What one execution attempt did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Executed {
    /// Nothing was queued.
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
        self.deps.clock.now_ms()
    }

    /// Take one queued response and run it to a terminal state.
    pub async fn run_once(&self, now_ms: u64) -> Executed {
        let agent = AgentId::new();

        let claimed = match self.deps.ledger.claim(agent, now_ms, self.cfg.exec_ttl).await {
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
        debug!(
            response = %claimed.record.response_id,
            attempt = ?claimed.record.attempt,
            "claim acquired; serving"
        );

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
    pub fn start(
        self: &Arc<Self>,
        max_concurrent: usize,
        poll_interval: Duration,
    ) -> AgentRuntimeHandle {
        let (stop_tx, mut stop_rx) = watch::channel(None::<Duration>);
        let runtime = self.clone();
        let join = tokio::spawn(async move {
            let permits = Arc::new(Semaphore::new(max_concurrent));
            let mut tasks = JoinSet::new();

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(poll_interval) => {
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

            // Drain in-flight work, bounded by the stop budget. Anything still running
            // past the budget is abandoned (aborted on JoinSet drop) and reclaimed by
            // the next startup's orphan reclaim (INV-45).
            let budget = *stop_rx.borrow();
            while !tasks.is_empty() {
                match budget {
                    Some(budget) => {
                        let deadline = tokio::time::Instant::now() + budget;
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
        // Traceability derived from the claim, so the fence the runner sees is the one
        // the ledger actually raised.
        let provenance = claimed.provenance();
        let record = claimed.record;
        let id = record.response_id.clone();
        let attempt = record.attempt;
        let started = std::time::Instant::now();
        debug!(
            response = %id,
            attempt = ?attempt,
            "serving a claimed response"
        );

        // Keep the claim alive across the whole (possibly long) run.
        let _heartbeat = spawn_heartbeat(
            self.deps.ledger.clone(),
            agent_id,
            self.deps.clock.clone(),
            self.cfg.heartbeat_interval,
        );

        // Announce the transition so a subscriber sees a defined progression.
        if let Err(e) = self
            .deps
            .event_log
            .append(AppendEvent::lifecycle_with_attempt(
                id.clone(),
                ResponseEventKind::InProgress,
                attempt,
                ResponseObject::without_output(&record),
            ))
            .await
        {
            warn!(
                response = %id,
                attempt = ?attempt,
                error = %e,
                "could not announce in_progress; subscribers stay on the created state"
            );
        }

        // History lives in the conversation snapshot (D30): read it once from the
        // anchor rather than walking a chain, then append this turn's own input.
        let snapshot = match self.resolve_snapshot(&record).await {
            Ok(ctx) => ctx,
            Err(e) => {
                warn!(response = %id, error = %e, "could not resolve context snapshot");
                // No sink exists yet, so there is no completed output to archive — only
                // the turn's input.
                return self
                    .fail(&record, attempt, Usage::default(), &[], &e.to_string(), now_ms)
                    .await;
            }
        };
        let mut items: Vec<ResponseItem> = snapshot.into_items();
        items.extend(record.spec.input_items.clone());

        let task = AgentTask {
            params: record.spec.params.clone(),
            items,
            provenance,
            max_tool_rounds: self.cfg.max_tool_rounds,
            ext: record.spec.ext.clone(),
        };

        let mut sink = EventSink::new(self.deps.event_log.clone(), id.clone(), attempt);

        let cancel = LedgerCancelProbe {
            ledger: self.deps.ledger.clone(),
            response_id: id.clone(),
            attempt,
            interval: self.cfg.cancel_poll_interval,
        };

        debug!(
            response = %id,
            runner = self.deps.runner.name(),
            items = task.items.len(),
            max_tool_rounds = task.max_tool_rounds,
            "handing the task to the runner"
        );
        let outcome = match self.deps.runner.run(&task, &mut sink, &cancel).await {
            Ok(outcome) => outcome,
            Err(AgentError::Superseded) => {
                info!(
                    response = %id,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "attempt superseded"
                );
                return Executed::Superseded;
            }
            Err(AgentError::Failed { message, usage }) => {
                warn!(
                    response = %id,
                    runner = self.deps.runner.name(),
                    error = %message,
                    "agent run failed"
                );
                // The runner failed, but items it already completed (tool calls, finished
                // text parts) are worth keeping — and so is the message it was still
                // streaming, reconstructed from the deltas the user already saw.
                let mut produced = sink.completed().to_vec();
                if let Some(partial) = sink.partial_item() {
                    produced.push(partial);
                }
                return self
                    .fail(&record, attempt, usage, &produced, &message, now_ms)
                    .await;
            }
        };

        if sink.stopped() {
            info!(
                response = %id,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "fence moved during generation; discarding output"
            );
            return Executed::Superseded;
        }

        debug!(
            response = %id,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "runner finished; committing the outcome"
        );
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

        debug!(
            response = %id,
            attempt = ?attempt,
            status = ?status,
            "committing a completed run to the ledger"
        );
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
        // A conversation-anchored response must persist its turn to the snapshot; a
        // bare response (no conversation) has no snapshot to append to, but it is still
        // "stored": its record and the terminal event (carrying the full object, output
        // included) are its retention under D30.
        let output_stored = if record.is_stored() {
            match record.conversation_id() {
                None => true,
                Some(_) => {
                    let reasoning = (!reasoning.is_empty()).then(|| reasoning.clone());
                    self.append_turn(record, &produced, reasoning, usage, status, now_ms)
                        .await
                }
            }
        } else {
            false
        };

        self.settle(record, status, output_stored, now_ms).await;

        if record.is_stored() && !output_stored {
            return Executed::Failed;
        }

        let kind = match status {
            ResponseStatus::Incomplete => ResponseEventKind::Incomplete,
            _ => ResponseEventKind::Completed,
        };
        // Re-read the record: `ledger.complete` has written the terminal status and
        // usage, and the terminal event must carry them (the claimed record still says
        // `in_progress`).
        let updated = match self.deps.ledger.get(id).await {
            Ok(Some(rec)) => rec,
            Ok(None) => record.clone(),
            Err(e) => {
                debug!(
                    response = %id,
                    error = %e,
                    "re-read after complete failed; terminal event carries the claimed record"
                );
                record.clone()
            }
        };
        self.close_stream(id, kind, ResponseObject::new(&updated, produced), now_ms)
            .await;
        info!(response = %id, "completed");
        Executed::Completed
    }

    /// Terminal funnel for a response that produced nothing usable.
    async fn fail(
        &self,
        record: &ResponseRecord,
        attempt: Attempt,
        usage: Usage,
        produced: &[ResponseItem],
        reason: &str,
        now_ms: u64,
    ) -> Executed {
        let id = &record.response_id;

        debug!(
            response = %id,
            attempt = ?attempt,
            "recording a failed run in the ledger"
        );
        if let Err(e) = self
            .deps
            .ledger
            .complete(id, attempt, ResponseStatus::Failed, usage, now_ms)
            .await
        {
            warn!(response = %id, error = %e, "could not record failure");
            return Executed::Failed;
        }

        // A failed turn still archives its input and whatever output it produced (D30
        // incomplete-turn archival), so the conversation chain keeps the user's
        // question, any finished tool calls, and the incomplete message that was
        // mid-stream when the failure hit (`produced` holds only `done` items plus
        // that reconstruction).
        if record.is_stored() && record.conversation_id().is_some() {
            self.append_turn(record, produced, None, usage, ResponseStatus::Failed, now_ms)
                .await;
        }

        self.settle(record, ResponseStatus::Failed, false, now_ms)
            .await;

        debug!(response = %id, reason, "failed");
        // Re-read for the same reason as the completion path: `ledger.complete` holds
        // the terminal status the failure event must report.
        let updated = match self.deps.ledger.get(id).await {
            Ok(Some(rec)) => rec,
            Ok(None) => record.clone(),
            Err(e) => {
                debug!(
                    response = %id,
                    error = %e,
                    "re-read after failure failed; terminal event carries the claimed record"
                );
                record.clone()
            }
        };
        self.close_stream(
            id,
            ResponseEventKind::Failed,
            ResponseObject::without_output(&updated),
            now_ms,
        )
        .await;
        Executed::Failed
    }

    /// Resolve the context a claim inherits, from the record's anchor (D30).
    ///
    /// `Previous` bare chains are mapped through the ledger to their conversation, then
    /// read here. `Root` yields empty. A bare chain with no conversation has no durable
    /// snapshot and is reported as broken.
    async fn resolve_snapshot(
        &self,
        record: &ResponseRecord,
    ) -> Result<ResolvedContext, SnapshotUnavailable> {
        let tenant = &record.tenant_id;
        debug!(response = %record.response_id, anchor = ?record.anchor(), "resolving the inherited context snapshot");
        match record.anchor() {
            ContextAnchor::Root => Ok(ResolvedContext::default()),
            ContextAnchor::Conversation(id) => Ok(self.read_snapshot(tenant, id).await?),
            ContextAnchor::Previous(id) => {
                let prev = self
                    .deps
                    .ledger
                    .get(id)
                    .await
                    .map_err(|e| {
                        debug!(
                            response = %id,
                            error = %e,
                            "chain resolution read failed; reporting as broken"
                        );
                        SnapshotUnavailable::ChainBroken(id.clone())
                    })?
                    .ok_or_else(|| SnapshotUnavailable::ChainBroken(id.clone()))?;
                match prev.anchor() {
                    ContextAnchor::Conversation(cid) => Ok(self.read_snapshot(tenant, cid).await?),
                    // Bare response: reconstruct its own input+output from the stream.
                    ContextAnchor::Root => self.reconstruct_bare(&prev).await,
                    ContextAnchor::Previous(_) => Err(SnapshotUnavailable::ChainBroken(id.clone())),
                }
            }
        }
    }

    /// Reconstruct a bare (conversation-less) response's input+output from the event
    /// stream: input from the record, output from the terminal event.
    async fn reconstruct_bare(
        &self,
        record: &ResponseRecord,
    ) -> Result<ResolvedContext, SnapshotUnavailable> {
        let mut items = record.spec.input_items.clone();
        let mut cursor: Option<u64> = None;
        loop {
            let batch = self
                .deps
                .event_log
                .read_after(
                    &record.response_id,
                    cursor,
                    EVENT_REPLAY_PAGE,
                    Duration::ZERO,
                )
                .await
                .map_err(|e| {
                    debug!(
                        response = %record.response_id,
                        error = %e,
                        "stream replay read failed; reporting chain as broken"
                    );
                    SnapshotUnavailable::ChainBroken(record.response_id.clone())
                })?;
            if batch.is_empty() {
                break;
            }
            let terminal = batch.iter().any(|e| e.kind().is_terminal());
            cursor = batch.last().map(|e| e.sequence_number());
            for event in &batch {
                // The lifecycle payload is a typed response object, so the output comes
                // back as items — no reaching into a JSON map by string key, and so no
                // parse failure to swallow either.
                if let Some(object) = event.response_object() {
                    items.extend(object.output.iter().cloned());
                }
            }
            if terminal {
                break;
            }
        }
        Ok(ResolvedContext::from_items(items, 1))
    }

    /// Read a conversation snapshot, reporting `Unavailable` when no conversation port
    /// is mounted (the runtime then fails the turn explicitly).
    async fn read_snapshot(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<ResolvedContext, ConversationError> {
        match &self.deps.conversations {
            Some(store) => store.read_snapshot(tenant, id).await,
            None => Err(ConversationError::Store(StoreError::Unavailable)),
        }
    }

    /// Append this turn's output to its conversation snapshot. Returns `true` when
    /// durably stored; `false` when there is no conversation to store into (a bare
    /// response has no durable home, so `store` cannot be honoured).
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
            (&self.deps.conversations, record.conversation_id())
        else {
            return false;
        };
        match store
            .append_turn(
                &record.tenant_id,
                conversation_id,
                &record.response_id,
                TurnCommit {
                    input_items: record.spec.input_items.clone(),
                    output_items: produced.to_vec(),
                    reasoning,
                    usage,
                    status,
                },
                now_ms,
            )
            .await
        {
            Ok(_) => {
                debug!(
                    response = %record.response_id,
                    conversation = %conversation_id,
                    "turn appended to the conversation snapshot"
                );
                true
            }
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

        let (Some(conversations), Some(conversation_id)) =
            (&self.deps.conversations, record.conversation_id())
        else {
            return;
        };

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

    /// Emit the terminal event (carrying the full response object, output included) and
    /// start the retention window.
    async fn close_stream(
        &self,
        id: &ResponseId,
        kind: ResponseEventKind,
        object: ResponseObject,
        now_ms: u64,
    ) {
        debug!(response = %id, kind = ?kind, "closing the event stream with a terminal event");
        if let Err(e) = self
            .deps
            .event_log
            .append(AppendEvent::lifecycle(id.clone(), kind, object))
            .await
        {
            warn!(
                response = %id,
                kind = ?kind,
                error = %e,
                "terminal event append failed; subscribers may hang until the stream closes"
            );
        }
        if let Err(e) = self
            .deps
            .event_log
            .close(id, now_ms, self.cfg.retain_after_terminal)
            .await
        {
            warn!(response = %id, error = %e, "retention window not started");
        }
    }
}

/// Active cancellation probe: polls the ledger fence (INV-6) until the attempt has been
/// superseded. The passive path — the sink refusing an append as stale — only fires when
/// a runner emits an event; a blocking tool call emits nothing for seconds, so the runner
/// races it against this probe.
struct LedgerCancelProbe {
    ledger: Arc<dyn ResponseClaimSource>,
    response_id: ResponseId,
    attempt: Attempt,
    interval: Duration,
}

#[async_trait::async_trait]
impl CancelProbe for LedgerCancelProbe {
    async fn cancelled(&self) {
        loop {
            match self
                .ledger
                .check_attempt(&self.response_id, self.attempt)
                .await
            {
                Ok(()) => {}
                Err(LedgerError::StaleAttempt) => return,
                Err(e) => {
                    warn!(
                        response = %self.response_id,
                        attempt = ?self.attempt,
                        error = %e,
                        "cancel-probe fence check failed; cancellation is now passive-only"
                    );
                }
            }
            tokio::time::sleep(self.interval).await;
        }
    }
}

/// Handle returned by [`AgentRuntime::start`].
pub struct AgentRuntimeHandle {
    stop_tx: watch::Sender<Option<Duration>>,
    join: tokio::task::JoinHandle<()>,
}

impl AgentRuntimeHandle {
    /// Stop gracefully: stop claiming new work, then wait for in-flight work to drain
    /// up to `drain_timeout`. Returns when the loop has exited.
    pub async fn stop(self, drain_timeout: Duration) {
        let _ = self.stop_tx.send(Some(drain_timeout));
        if let Err(e) = self.join.await {
            warn!(error = %e, "serve loop task failed");
        }
    }
}

/// RAII guard that aborts the background heartbeat task when dropped.
struct HeartbeatGuard {
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for HeartbeatGuard {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Spawn a background task that keeps the claim's heartbeat fresh while the runner
/// executes the (possibly long) ReAct loop.
fn spawn_heartbeat(
    ledger: Arc<dyn ResponseClaimSource>,
    agent_id: AgentId,
    clock: Arc<dyn Clock>,
    interval: Duration,
) -> HeartbeatGuard {
    let interval = interval.max(MIN_HEARTBEAT_INTERVAL);
    let handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            match ledger.heartbeat(agent_id, clock.now_ms()).await {
                Ok(()) => {}
                Err(LedgerError::Store(StoreError::ReadOnly)) => {
                    debug!(
                        "heartbeat stopped: ledger is read-only; the claim will be reaped"
                    );
                    break;
                }
                Err(e) => warn!(error = %e, "heartbeat failed"),
            }
        }
    });
    HeartbeatGuard { handle }
}
