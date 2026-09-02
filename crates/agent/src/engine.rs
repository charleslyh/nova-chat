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

use async_trait::async_trait;
use nova_responses_core::{
    validate_outcome, AgentId, Attempt, ClaimedResponse, CompletionsRequest,
    CompletionsRequestScheduler, CompletionsSink, ContextStore, EventBody, FinishReason, NodeTag,
    RequestProvenance, ResponseEvent, ResponseEventKind, ResponseEventLog, ResponseId,
    ResponseItem, ResponseLedger, ResponseStatus, SchedulerError, SinkError, SinkVerdict,
    TenantId, ToolExecutor, ToolSpec, Usage,
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
    /// This node. Claiming is scoped to it (FR-4).
    pub node_tag: NodeTag,
}

#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub exec_ttl_ms: u64,
    pub chain_limits: ChainLimits,
    pub retain_after_terminal_ms: u64,
    /// Functions offered to the model: *what it may call*. The executor in
    /// [`AgentDeps`] is *what carries the call out*.
    pub tool_specs: Vec<ToolSpec>,
    /// Hard ceiling on tool-calling rounds per response. A model that never
    /// stops asking for tools reaches [`ResponseStatus::Incomplete`], not an
    /// unbounded loop.
    pub max_tool_rounds: usize,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            exec_ttl_ms: 300_000,
            chain_limits: ChainLimits::default(),
            retain_after_terminal_ms: 60_000,
            tool_specs: Vec::new(),
            max_tool_rounds: 20,
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
            .claim(&self.deps.node_tag, agent, now_ms, self.cfg.exec_ttl_ms)
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

        self.serve(claimed, now_ms).await
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

    async fn serve(&self, claimed: ClaimedResponse, now_ms: u64) -> Executed {
        let record = claimed.record;
        let id = record.response_id.clone();
        let tenant = record.tenant_id.clone();
        let attempt = claimed.attempt;
        let stored = record.stored;

        // Announce the transition so a subscriber attached from the start sees a
        // defined progression rather than a gap.
        let _ = self
            .deps
            .event_log
            .append(ResponseEvent::lifecycle_with_attempt(
                id.clone(),
                ResponseEventKind::InProgress,
                attempt,
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
        };

        loop {
            let request = match CompletionsRequest::from_context(
                record.model.clone(),
                record.instructions.as_deref(),
                &conversation,
                provenance.clone(),
            )
            .map(|r| r.with_tools(self.cfg.tool_specs.clone()))
            {
                Ok(r) => r,
                Err(e) => {
                    warn!(response = %id, error = %e, "cannot build a request");
                    return self
                        .fail(&id, &tenant, attempt, usage, &format!("context: {e}"), now_ms)
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
                    return self.fail(&id, &tenant, attempt, usage, &e.to_string(), now_ms).await;
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
                return self.fail(&id, &tenant, attempt, usage, &e.to_string(), now_ms).await;
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
                            &id,
                            &tenant,
                            attempt,
                            stored,
                            produced,
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
                            &id,
                            &tenant,
                            attempt,
                            stored,
                            produced,
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
                                &id,
                                &tenant,
                                attempt,
                                stored,
                                produced,
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
                                &id,
                                &tenant,
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
                                        .fail(&id, &tenant, attempt, usage, &e.to_string(), now_ms)
                                        .await;
                                }
                                if sink.stopped {
                                    return Executed::Superseded;
                                }
                                if let Err(e) = sink.output_item_done(&item).await {
                                    return self
                                        .fail(&id, &tenant, attempt, usage, &e.to_string(), now_ms)
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
                                    .fail(&id, &tenant, attempt, usage, &e.to_string(), now_ms)
                                    .await;
                            }
                        }
                    }
                    // Loop: the model now sees the call and its output.
                }
            }
        }
    }

    async fn complete(
        &self,
        id: &ResponseId,
        tenant: &TenantId,
        attempt: Attempt,
        stored: bool,
        produced: Vec<ResponseItem>,
        usage: Usage,
        status: ResponseStatus,
        now_ms: u64,
    ) -> Executed {
        if let Err(e) = self
            .deps
            .ledger
            .complete(id, attempt, status, usage, now_ms)
            .await
        {
            warn!(response = %id, error = %e, "complete failed");
            return Executed::Failed;
        }

        // Second, independent write path. Deliberately not derived from the event
        // stream: that buffer is bounded and transient, so durable history may not
        // depend on it (FR-20 / INV-48). The tool-call trace is part of `produced`,
        // so the next turn's snapshot sees the whole agent loop.
        if stored {
            if let Err(e) = self
                .deps
                .context
                .append_output(tenant, id, produced, usage, status, now_ms)
                .await
            {
                warn!(response = %id, error = %e, "storing output failed");
                return Executed::Failed;
            }
        }

        let kind = match status {
            ResponseStatus::Incomplete => ResponseEventKind::Incomplete,
            _ => ResponseEventKind::Completed,
        };
        self.close_stream(id, kind, now_ms).await;
        info!(response = %id, "completed");
        Executed::Completed
    }

    async fn fail(
        &self,
        id: &ResponseId,
        tenant: &TenantId,
        attempt: Attempt,
        usage: Usage,
        reason: &str,
        now_ms: u64,
    ) -> Executed {
        let _ = tenant;
        if let Err(e) = self
            .deps
            .ledger
            .complete(id, attempt, ResponseStatus::Failed, usage, now_ms)
            .await
        {
            warn!(response = %id, error = %e, "could not record failure");
            return Executed::Failed;
        }
        debug!(response = %id, reason, "failed");
        self.close_stream(id, ResponseEventKind::Failed, now_ms)
            .await;
        Executed::Failed
    }

    /// Emit the terminal event and start the retention window.
    async fn close_stream(
        &self,
        id: &ResponseId,
        kind: ResponseEventKind,
        now_ms: u64,
    ) {
        // `attempt: None` on purpose. The fence was already checked by the ledger
        // transition; checking it again here would reject the very event that
        // announces the transition, and the stream would never terminate.
        let _ = self
            .deps
            .event_log
            .append(ResponseEvent::lifecycle(id.clone(), kind))
            .await;
        let _ = self
            .deps
            .event_log
            .close(id, now_ms, self.cfg.retain_after_terminal_ms)
            .await;
    }
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
                delta: text.to_string(),
            },
        )
        .await
    }

    async fn output_item_added(&mut self, item: &ResponseItem) -> Result<SinkVerdict, SinkError> {
        let index = self.output_index;
        self.output_index += 1;
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
        _item_id: &str,
        delta: &str,
    ) -> Result<SinkVerdict, SinkError> {
        self.push(
            ResponseEventKind::FunctionCallArgumentsDelta,
            EventBody::Delta {
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
}
