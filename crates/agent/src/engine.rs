//! In-process agent: claim a response, run its ReAct loop, submit the result.
//!
//! # Why in-process (not a pull protocol)
//!
//! Execution used to be an external process polling `/v1/agent/claim`. That model
//! carried a hidden assumption — **the claiming node is the host node** — which the
//! in-memory backend satisfied for free, because each node held its own ledger.
//!
//! Making the ledger shared (D21) removed that guarantee and nothing expressed it.
//! A worker attached to node-a could claim node-b's response; its increments then
//! landed in node-a's in-flight buffer, while subscribers, routing by the node tag
//! inside the id, were sent to node-b. They saw `Created` and then silence, with no
//! error anywhere — indistinguishable from a model that produced nothing.
//!
//! Executing in the creating node makes producer and buffer holder **the same
//! process by construction** (D23), so no mechanism is needed to keep them aligned.
//!
//! # A response is a ReAct loop, not one completions call
//!
//! The model may answer directly, or it may call tools and answer later — possibly
//! several times. The loop below is the agent:
//!
//! ```text
//! claim
//!   → build CompletionsRequest from materialised history
//!   → schedule
//!       ├─ Stop / Refusal → complete with everything produced
//!       ├─ Length         → complete as Incomplete (truncated)
//!       └─ ToolCalls      → run each tool, append its output, loop
//! ```
//!
//! Every item the loop produces — tool calls **and** their outputs included — is
//! committed as output, so the next turn's snapshot carries the whole trace (D24),
//! exactly as a multi-round agent conversation requires. Without this, a tool-using
//! turn would be stored as a bare `function_call` with no result and no follow-up,
//! and the next turn's model would read a conversation that never happened.
//!
//! # Still not IO
//!
//! The agent talks to ports only: the scheduler for completions and the tool
//! executor for tool calls. With the in-memory adapters and mock ports, the whole
//! path — claim, multi-round loop, submit, abandon on a moved fence, fail loudly on
//! a bad outcome or a tool error — runs in a unit test with no socket and no model.
//!
//! # What stays, and why
//!
//! The attempt fence survives the loop. A task stalled past its deadline is reaped
//! by the sweeper; if it later wakes and appends, its attempt is stale and the write
//! must be refused (FR-6 / CR-7 / INV-6). "No concurrent claim" does not imply
//! "no concurrent write".

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nova_responses_core::{
    validate_outcome, AgentId, Attempt, ClaimedResponse, Clock, CompletionsRequest,
    CompletionsRequestScheduler, CompletionsSink, ContextStore, ConversationStore, EventBody,
    FinishReason, RequestProvenance, ResponseEvent, ResponseEventKind, ResponseEventLog,
    ResponseId, ResponseItem, ResponseLedger, ResponseStatus, SchedulerError, SinkError,
    SinkVerdict, StoredResponse, TenantId, ToolExecutor, Usage,
};
use nova_responses_core::{ChainLimits, LedgerError};
use tracing::{debug, info, warn};

/// Ports the agent needs.
pub struct AgentDeps {
    pub ledger: Arc<dyn ResponseLedger>,
    pub event_log: Arc<dyn ResponseEventLog>,
    pub context: Arc<dyn ContextStore>,
    pub scheduler: Arc<dyn CompletionsRequestScheduler>,
    /// Carries tool calls out. `NoopToolExecutor` is the explicit "no tools"
    /// instance; a real registry or a remote bridge is the same shape.
    pub tools: Arc<dyn ToolExecutor>,
    /// Wall clock in production; a virtual clock the test can advance (D15).
    /// The background heartbeat task reads it for timestamps and interval sleeps.
    pub clock: Arc<dyn Clock>,

    /// Conversation bookkeeping at terminal (D28): releasing the in-flight marker
    /// and advancing the tail. `Option` rather than a null object, unlike `tools`,
    /// because "no port mounted" and "this response has no conversation" are two
    /// different things; a null object would answer both silently.
    pub conversations: Option<Arc<dyn ConversationStore>>,
}

#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub exec_ttl_ms: u64,
    pub chain_limits: ChainLimits,
    pub retain_after_terminal_ms: u64,
    /// Hard ceiling on tool-calling rounds per response. A model that never
    /// stops asking for tools reaches [`ResponseStatus::Incomplete`], not an
    /// unbounded loop.
    pub max_tool_rounds: usize,
    /// Interval between keep-alive heartbeats sent while the (possibly long)
    /// ReAct loop runs. Must be shorter than the sweeper's `heartbeat_ttl_ms`,
    /// otherwise a generation longer than that TTL is reaped mid-flight. The
    /// actual interval is clamped to a sane floor to avoid a hot spin.
    pub heartbeat_interval_ms: u64,
}

impl Default for AgentConfig {
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

/// What one execution attempt did. Every variant is a distinct outcome, so a test
/// can assert on the path taken rather than only on the final state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Executed {
    /// Nothing was queued for this node.
    Idle,
    Completed,
    /// Abandoned because the fence moved. **Not** a failure: the work belongs to
    /// another attempt now, and reporting it as failed would terminate a response
    /// that attempt is actively serving.
    Superseded,
    /// Reported as failed, so the response reaches a terminal state immediately
    /// rather than waiting to be reclaimed.
    Failed,
}

/// Drives execution for one node: claim, run the ReAct loop, submit.
pub struct Agent {
    deps: AgentDeps,
    cfg: AgentConfig,
}

impl Agent {
    pub fn new(deps: AgentDeps, cfg: AgentConfig) -> Self {
        Self { deps, cfg }
    }

    pub fn scheduler_name(&self) -> &str {
        self.deps.scheduler.name()
    }

    /// Take one queued response owned by this node and run it to a terminal state.
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
                // Degraded to read-only: no new work may start. Not an error to
                // report, and nothing was claimed.
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

    /// Drain everything currently queued for this node.
    ///
    /// Used at startup so work accepted just before a restart is not left waiting
    /// for the next external trigger.
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

    async fn serve(&self, claimed: ClaimedResponse, agent_id: AgentId, now_ms: u64) -> Executed {
        let record = claimed.record;
        // Only the id is lifted out, for logging. Everything the terminal funnels
        // need travels as `&record`, so they cannot be handed a mismatched pair.
        let id = record.response_id.clone();
        let attempt = claimed.attempt;

        // Keep the claim alive across the whole (possibly long) ReAct loop. The
        // sweeper reaps a claim whose owner stops heartbeating past its TTL, which
        // would raise the fence and turn an in-flight generation into `Superseded`.
        // The guard aborts the heartbeat task when `serve` returns on any path.
        let _heartbeat = spawn_heartbeat(
            self.deps.ledger.clone(),
            agent_id,
            self.deps.clock.clone(),
            self.cfg.heartbeat_interval_ms,
        );

        // Announce the transition so a subscriber attached from the start sees a
        // defined progression rather than a gap.
        let _ = self
            .deps
            .event_log
            .append(ResponseEvent::lifecycle_with_attempt(
                id.clone(),
                ResponseEventKind::InProgress,
                attempt,
                record.to_response_value(),
            ))
            .await;

        // History is materialised onto the record as a flat copy (D24): read the
        // snapshot rather than walking the chain. The scheduler has no tenant
        // context and must never resolve history itself. Because the snapshot is
        // self-contained, a later deletion of an ancestor cannot strand this
        // response — it already holds everything it needs.
        //
        // `conversation` is the running transcript fed to the model each round;
        // `base_len` marks where this response's own production begins, so the
        // final output can be sliced out without a second parallel buffer.
        let mut conversation: Vec<ResponseItem> = record.context.clone();
        conversation.extend(record.input_items.clone());
        let base_len = conversation.len();

        let mut usage = Usage::default();

        let provenance = RequestProvenance {
            response_id: id.to_string(),
            attempt: attempt.0,
            exec_deadline_ms: claimed.exec_deadline_ms,
        };

        let mut tool_rounds = 0usize;
        // One sink for the whole loop: it carries the fencing `attempt` and the
        // `stopped` latch, so a fence that moves mid-round also stops later
        // rounds' tool-result writes.
        let mut sink = LedgerSink {
            event_log: self.deps.event_log.clone(),
            ledger: self.deps.ledger.clone(),
            response_id: id.clone(),
            attempt,
            stopped: false,
            output_index: 0,
            current_item_id: None,
            current_content_index: None,
            reasoning: String::new(),
        };

        loop {
            let request = match CompletionsRequest::from_context(
                record.model.clone(),
                record.instructions.as_deref(),
                &conversation,
                provenance.clone(),
            )
            // Tools come from the caller's per-response declaration, not a static
            // deployment config (single source of truth: `record.tools` and
            // `record.tool_choice`).
            .map(|r| r.with_tools(record.tools.clone()).with_tool_choice(record.tool_choice.clone()))
            {
                Ok(r) => r,
                Err(e) => {
                    warn!(response = %id, error = %e, "cannot build a request");
                    return self
                        .fail(&record, attempt, usage, &format!("context: {e}"), now_ms)
                        .await;
                }
            };

            let outcome = match self.deps.scheduler.schedule(&request, &mut sink).await {
                Ok(outcome) => outcome,
                Err(SchedulerError::Superseded) => {
                    info!(response = %id, "attempt superseded");
                    return Executed::Superseded;
                }
                Err(e) => {
                    warn!(
                        response = %id,
                        scheduler = self.deps.scheduler.name(),
                        retryable = e.is_retryable(),
                        error = %e,
                        "scheduling failed"
                    );
                    return self.fail(&record, attempt, usage, &e.to_string(), now_ms).await;
                }
            };

            if sink.stopped {
                info!(response = %id, "fence moved during generation; discarding output");
                return Executed::Superseded;
            }

            if let Err(e) = validate_outcome(&outcome) {
                warn!(
                    response = %id,
                    scheduler = self.deps.scheduler.name(),
                    error = %e,
                    "scheduler produced an unusable outcome"
                );
                return self.fail(&record, attempt, usage, &e.to_string(), now_ms).await;
            }

            // Pull the tool calls out before `outcome.items` is moved into the
            // transcript. Owned copies, so the borrow does not outlive the move.
            let calls: Vec<(String, String, String)> = outcome
                .items
                .iter()
                .filter_map(|item| match item {
                    ResponseItem::FunctionCall {
                        call_id,
                        name,
                        arguments,
                        ..
                    } => Some((call_id.clone(), name.clone(), arguments.clone())),
                    _ => None,
                })
                .collect();

            let finish = outcome.finish;

            match finish {
                FinishReason::Stop | FinishReason::Refusal => {
                    usage = usage.add(outcome.usage);
                    conversation.extend(outcome.items);
                    let produced = conversation[base_len..].to_vec();
                    return self
                        .complete(
                            &record,
                            attempt,
                            produced,
                            sink.reasoning.clone(),
                            usage,
                            ResponseStatus::Completed,
                            now_ms,
                        )
                        .await;
                }

                FinishReason::Length => {
                    // Truncated. Succeeded at the transport level but must not be
                    // stored as a complete answer (CR-12).
                    usage = usage.add(outcome.usage);
                    conversation.extend(outcome.items);
                    let produced = conversation[base_len..].to_vec();
                    return self
                        .complete(
                            &record,
                            attempt,
                            produced,
                            sink.reasoning.clone(),
                            usage,
                            ResponseStatus::Incomplete,
                            now_ms,
                        )
                        .await;
                }

                FinishReason::ToolCalls => {
                    if tool_rounds >= self.cfg.max_tool_rounds {
                        // Ceiling reached: the model keeps asking for tools. Book
                        // the tokens it spent, but drop this round's call — there
                        // is no output for it, and a bare `function_call` with no
                        // result would corrupt the next turn's transcript.
                        usage = usage.add(outcome.usage);
                        let produced = conversation[base_len..].to_vec();
                        return self
                            .complete(
                                &record,
                                attempt,
                                produced,
                                sink.reasoning.clone(),
                                usage,
                                ResponseStatus::Incomplete,
                                now_ms,
                            )
                            .await;
                    }
                    tool_rounds += 1;
                    if calls.is_empty() {
                        // The scheduler claimed tool calls but produced none. A
                        // silent re-loop would spin to the round ceiling and
                        // report Incomplete for what is a malformed outcome.
                        return self
                            .fail(
                                &record,
                                attempt,
                                usage,
                                "finish=ToolCalls but no tool calls were produced",
                                now_ms,
                            )
                            .await;
                    }
                    usage = usage.add(outcome.usage);
                    conversation.extend(outcome.items);
                    for (call_id, name, arguments) in calls {
                        match self.deps.tools.call(&name, &arguments).await {
                            Ok(output) => {
                                let item = ResponseItem::FunctionCallOutput {
                                    call_id,
                                    output,
                                    id: None,
                                    status: None,
                                };
                                // Announce the tool result on the stream, so a
                                // subscriber sees it live rather than only at
                                // terminal via `GET`.
                                if let Err(e) = sink.output_item_added(&item).await {
                                    return self
                                        .fail(&record, attempt, usage, &e.to_string(), now_ms)
                                        .await;
                                }
                                if sink.stopped {
                                    return Executed::Superseded;
                                }
                                if let Err(e) = sink.output_item_done(&item).await {
                                    return self
                                        .fail(&record, attempt, usage, &e.to_string(), now_ms)
                                        .await;
                                }
                                conversation.push(item);
                            }
                            Err(e) => {
                                warn!(
                                    response = %id,
                                    tool = %name,
                                    executor = self.deps.tools.name(),
                                    error = %e,
                                    "tool execution failed"
                                );
                                return self
                                    .fail(&record, attempt, usage, &e.to_string(), now_ms)
                                    .await;
                            }
                        }
                    }
                    // Loop: the model now sees the call and its output.
                }
            }
        }
    }

    /// Terminal funnel for a response that produced output.
    ///
    /// Takes the whole record rather than `(id, tenant, stored, …)` picked apart:
    /// those are three fields of one thing, and passing them separately makes it
    /// possible to pair an id with the wrong tenant — which the ports would then
    /// read as "not found" and no test would notice.
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
            // The ledger did not move, so nothing downstream is owed — including
            // the session lock, which some later terminal path will release.
            return Executed::Failed;
        }

        // Past this point the ledger says terminal, so the session lock **must**
        // be released whatever the remaining writes do. That is why the result of
        // the output write is captured rather than returned on: an early return
        // here is exactly the path that would leave a session busy forever.
        //
        // Second, independent write path. Deliberately not derived from the event
        // stream: that buffer is bounded and transient, so durable history may not
        // depend on it (FR-20 / INV-48). The tool-call trace is part of `produced`,
        // so the next turn's snapshot sees the whole agent loop.
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

        // The tail advances only when this turn's output actually landed. Pointing
        // a conversation at a response whose output is missing would give the next
        // turn a transcript that ends mid-question.
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

        // Lock released, tail **not** advanced: a failed turn committed no output,
        // so advancing would leave the conversation ending on an unanswered
        // question. The caller retrying against the same conversation gets the
        // same context it had, which is the point.
        self.settle(record, ResponseStatus::Failed, false, now_ms)
            .await;

        debug!(response = %id, reason, "failed");
        self.close_stream(id, tenant, ResponseEventKind::Failed, now_ms)
            .await;
        Executed::Failed
    }

    /// Release the turn lock, and advance the conversation tail when asked.
    ///
    /// Order matters: the tail is advanced **before** the lock is released.
    /// Reversed, a client that sees `turn_completed` and immediately starts the
    /// next turn could read a tail that has not moved yet, and the new turn would
    /// silently lose the one just finished.
    ///
    /// Failures here are logged and not propagated. The caller's outcome is about
    /// its generation, and masking that with a bookkeeping error would hide the
    /// real cause; the lock is also not lost for good, since the reap path
    /// releases it too.
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
            // Advance the tail **first**. A subscriber that sees `turn_completed`
            // and immediately reads the transcript (or starts the next turn) must
            // find the output already there. Reversed, it could read a tail that
            // has not moved yet, and silently lose the turn just finished.
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
            // Release the in-flight marker (atomically emitting `turn_completed`),
            // on every terminal path. Failures are logged, not propagated: the
            // marker is recovered by the admission-side stale takeover.
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
        // `attempt: None` on purpose. The fence was already checked by the ledger
        // transition; checking it again here would reject the very event that
        // announces the transition, and the stream would never terminate.
        //
        // The terminal record is read back so the event's `response` object
        // reflects the finished state. A store miss (store=false, or an
        // unavailable store) falls back to a minimal envelope.
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
            .append(ResponseEvent::lifecycle(id.clone(), kind, response))
            .await;
        let _ = self
            .deps
            .event_log
            .close(id, now_ms, self.cfg.retain_after_terminal_ms)
            .await;
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
/// agent runs the (possibly long) ReAct loop.
///
/// The sweeper reaps a claim whose owner stops heartbeating past
/// `heartbeat_ttl_ms`; without this task, any generation longer than that TTL
/// would be reaped mid-flight — its fence raised and its writes refused as
/// stale. The interval must be shorter than the sweeper's TTL (deployment
/// invariant, not enforced here).
fn spawn_heartbeat(
    ledger: Arc<dyn ResponseLedger>,
    agent_id: AgentId,
    clock: Arc<dyn Clock>,
    interval_ms: u64,
) -> HeartbeatGuard {
    let interval_ms = interval_ms.max(MIN_HEARTBEAT_INTERVAL_MS);
    let handle = tokio::spawn(async move {
        // First beat comes after one interval: `claim` already recorded one.
        // The interval is wall-clock (tokio time, pausable in tests), while the
        // timestamp read for the beat is the injected `Clock` — the two stay
        // independent so a test can advance time and reap deterministically.
        loop {
            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
            match ledger.heartbeat(agent_id, clock.now_ms().await).await {
                Ok(()) => {}
                // Read-only degrade: the ledger will not accept writes, and the
                // reap path will clean up the abandoned claim.
                Err(LedgerError::ReadOnly) => break,
                Err(e) => warn!(error = %e, "heartbeat failed"),
            }
        }
    });
    HeartbeatGuard { handle }
}

/// Streams scheduler output into this node's in-flight buffer.
struct LedgerSink {
    event_log: Arc<dyn ResponseEventLog>,
    ledger: Arc<dyn ResponseLedger>,
    response_id: ResponseId,
    attempt: Attempt,
    stopped: bool,
    /// Index of the output item currently being streamed, mirroring the
    /// `output_index` of the OpenAI `response.output_item.*` events.
    output_index: u32,
    /// Id of the output item currently being streamed, carried on delta events
    /// as `item_id` (the message id or a tool `call_id`).
    current_item_id: Option<String>,
    /// Index of the content part currently being streamed, carried on text
    /// delta/done events as `content_index`.
    current_content_index: Option<u32>,
    /// Reasoning / thinking text accumulated across the whole loop, to be
    /// persisted with the final output (render-only, never re-enters context).
    reasoning: String,
}

/// The stream identity of an output item: a tool `call_id` for tool items, the
/// message id otherwise (empty when absent — the scheduler assigns one via
/// [`assistant_text_message`]).
fn item_id_of(item: &ResponseItem) -> String {
    match item {
        ResponseItem::FunctionCall { call_id, .. }
        | ResponseItem::FunctionCallOutput { call_id, .. } => call_id.clone(),
        ResponseItem::Message { id, .. } => id.clone().unwrap_or_default(),
    }
}

impl LedgerSink {
    async fn push(
        &mut self,
        kind: ResponseEventKind,
        body: EventBody,
    ) -> Result<SinkVerdict, SinkError> {
        if self.stopped {
            return Ok(SinkVerdict::Stop);
        }
        // The attempt travels with the event, so the log can refuse a write from a
        // superseded holder (INV-6).
        match self
            .event_log
            .append(ResponseEvent {
                response_id: self.response_id.clone(),
                sequence_number: 0,
                kind,
                attempt: Some(self.attempt),
                body,
            })
            .await
        {
            Ok(_) => Ok(SinkVerdict::Continue),
            Err(nova_responses_core::EventLogError::StaleAttempt) => {
                self.stopped = true;
                Ok(SinkVerdict::Stop)
            }
            Err(e) => Err(SinkError::Transport(e.to_string())),
        }
    }

    /// Push an output-item event, carrying the item nested as `item`.
    async fn push_item(
        &mut self,
        kind: ResponseEventKind,
        output_index: u32,
        item: &ResponseItem,
    ) -> Result<SinkVerdict, SinkError> {
        let item = serde_json::to_value(item).unwrap_or_default();
        self.push(kind, EventBody::Item { output_index, item })
            .await
    }
}

#[async_trait]
impl CompletionsSink for LedgerSink {
    async fn text_delta(&mut self, text: &str) -> Result<SinkVerdict, SinkError> {
        let _ = &self.ledger;
        self.push(
            ResponseEventKind::OutputTextDelta,
            EventBody::Delta {
                item_id: self.current_item_id.clone().unwrap_or_default(),
                output_index: self.output_index.saturating_sub(1),
                content_index: self.current_content_index,
                delta: text.to_string(),
            },
        )
        .await
    }

    async fn reasoning_text_delta(&mut self, text: &str) -> Result<SinkVerdict, SinkError> {
        // Render-only on the stream, but accumulated so it can be persisted with
        // the final output: reasoning carries no item id / output index because
        // it is not an output item, yet a later re-render must reproduce it.
        self.reasoning.push_str(text);
        self.push(
            ResponseEventKind::ReasoningTextDelta,
            EventBody::Delta {
                item_id: String::new(),
                output_index: 0,
                content_index: None,
                delta: text.to_string(),
            },
        )
        .await
    }

    async fn output_item_added(&mut self, item: &ResponseItem) -> Result<SinkVerdict, SinkError> {
        let index = self.output_index;
        self.output_index += 1;
        self.current_item_id = Some(item_id_of(item));
        self.push_item(ResponseEventKind::OutputItemAdded, index, item)
            .await
    }

    async fn output_item_done(&mut self, item: &ResponseItem) -> Result<SinkVerdict, SinkError> {
        let index = self.output_index.saturating_sub(1);
        self.push_item(ResponseEventKind::OutputItemDone, index, item)
            .await
    }

    async fn function_call_arguments_delta(
        &mut self,
        item_id: &str,
        delta: &str,
    ) -> Result<SinkVerdict, SinkError> {
        self.push(
            ResponseEventKind::FunctionCallArgumentsDelta,
            EventBody::Delta {
                item_id: item_id.to_string(),
                output_index: self.output_index.saturating_sub(1),
                content_index: None,
                delta: delta.to_string(),
            },
        )
        .await
    }

    async fn function_call_arguments_done(
        &mut self,
        item_id: &str,
        arguments: &str,
    ) -> Result<SinkVerdict, SinkError> {
        let index = self.output_index.saturating_sub(1);
        self.push(
            ResponseEventKind::FunctionCallArgumentsDone,
            EventBody::Arguments {
                output_index: index,
                item_id: item_id.to_string(),
                arguments: arguments.to_string(),
            },
        )
        .await
    }

    async fn content_part_added(
        &mut self,
        item_id: &str,
        content_index: u32,
    ) -> Result<SinkVerdict, SinkError> {
        self.current_content_index = Some(content_index);
        let part = serde_json::json!({ "type": "output_text", "text": "" });
        self.push(
            ResponseEventKind::ContentPartAdded,
            EventBody::Part {
                item_id: item_id.to_string(),
                output_index: self.output_index.saturating_sub(1),
                content_index,
                part,
            },
        )
        .await
    }

    async fn output_text_done(&mut self, text: &str) -> Result<SinkVerdict, SinkError> {
        self.push(
            ResponseEventKind::OutputTextDone,
            EventBody::Text {
                item_id: self.current_item_id.clone().unwrap_or_default(),
                output_index: self.output_index.saturating_sub(1),
                content_index: self.current_content_index.unwrap_or_default(),
                text: text.to_string(),
            },
        )
        .await
    }

    async fn content_part_done(
        &mut self,
        item_id: &str,
        content_index: u32,
        text: &str,
    ) -> Result<SinkVerdict, SinkError> {
        let part = serde_json::json!({ "type": "output_text", "text": text });
        self.push(
            ResponseEventKind::ContentPartDone,
            EventBody::Part {
                item_id: item_id.to_string(),
                output_index: self.output_index.saturating_sub(1),
                content_index,
                part,
            },
        )
        .await
    }
}
