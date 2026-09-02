//! Outbound completions port.
//!
//! # Why "scheduler" rather than "executor"
//!
//! The name widens what an implementation is allowed to do, deliberately. An
//! *executor* would suggest "issue this request now"; a *scheduler* is free to
//! queue, rate-limit, batch, retry against a second provider, or reuse a
//! connection pool — all of which are legitimate concerns of the outbound edge and
//! none of which the caller should know about.
//!
//! The consequence to be aware of: **concurrency policy belongs behind this
//! trait**, not in the work loop that calls it. A worker that also throttled would
//! make provider limits a property of the fleet's shape, so changing provider would
//! mean re-tuning deployment.
//!
//! # Direction: this is the outbound edge
//!
//! Not to be confused with the execution-side work loop, which faces *our own*
//! gateway. The two point in opposite directions and neither belongs inside the
//! other:
//!
//! ```text
//!   gateway  ◀── claim/append/complete ──  ExecutionWorker  ── request ──▶  provider
//!            (inbound: our protocol)                        (outbound: this port)
//! ```
//!
//! # Implementations live in `crates/adapters/*`
//!
//! As with every other port here. `adapters/completions-mock` provides the
//! model-free implementations that make integration tests cheap; a real provider
//! is another adapter, added without touching this file or any caller.

use async_trait::async_trait;
use thiserror::Error;

use crate::completions::{CompletionsOutcome, CompletionsRequest, ToolCall};
use crate::protocol::ResponseItem;

/// Schedules and fulfils completions requests.
///
/// Implementations must be `Send + Sync` so one scheduler can serve a whole fleet
/// of concurrent workers; per-request state belongs in locals, not in `self`.
#[async_trait]
pub trait CompletionsRequestScheduler: Send + Sync {
    /// Stable identifier for logs and metrics, e.g. `"openai"`, `"scripted"`.
    fn name(&self) -> &str;

    /// Fulfil `request`, pushing incremental output to `sink` as it arrives.
    ///
    /// # The contract on `sink`
    ///
    /// `sink` returns a [`SinkVerdict`]. On [`SinkVerdict::Stop`] the
    /// implementation **must stop and return [`SchedulerError::Superseded`]** — not
    /// finish the request, not return an outcome. `Stop` means the fencing token
    /// moved: this attempt's output would interleave with a newer one, and every
    /// further token is billed against work that will be discarded.
    ///
    /// # The contract on the return value
    ///
    /// [`CompletionsOutcome::items`] is the considered final answer. It is not
    /// required to equal the concatenation of streamed deltas, and callers must not
    /// reconstruct it from them (FR-20 / INV-48).
    ///
    /// Streaming nothing is valid: a refusal or an immediate tool call may produce
    /// no text at all.
    async fn schedule(
        &self,
        request: &CompletionsRequest,
        sink: &mut dyn CompletionsSink,
    ) -> Result<CompletionsOutcome, SchedulerError>;
}

/// Receives incremental output during [`CompletionsRequestScheduler::schedule`].
#[async_trait]
pub trait CompletionsSink: Send {
    /// Push a text fragment. Fragment boundaries carry no meaning and need not
    /// align to tokens, words, or grapheme clusters.
    async fn text_delta(&mut self, text: &str) -> Result<SinkVerdict, SinkError>;

    /// A new output item appears.
    ///
    /// For a `function_call` this precedes its argument deltas; for a
    /// `function_call_output` it announces a tool result. Default: ignore.
    async fn output_item_added(&mut self, _item: &ResponseItem) -> Result<SinkVerdict, SinkError> {
        Ok(SinkVerdict::Continue)
    }

    /// An output item is complete — its content is final.
    ///
    /// Default: ignore.
    async fn output_item_done(&mut self, _item: &ResponseItem) -> Result<SinkVerdict, SinkError> {
        Ok(SinkVerdict::Continue)
    }

    /// A fragment of a `function_call`'s arguments, streamed as it is produced.
    ///
    /// Default: ignore.
    async fn function_call_arguments_delta(
        &mut self,
        _item_id: &str,
        _delta: &str,
    ) -> Result<SinkVerdict, SinkError> {
        Ok(SinkVerdict::Continue)
    }

    /// The complete arguments of a `function_call`, ending its delta stream.
    ///
    /// Default: ignore.
    async fn function_call_arguments_done(
        &mut self,
        _item_id: &str,
        _arguments: &str,
    ) -> Result<SinkVerdict, SinkError> {
        Ok(SinkVerdict::Continue)
    }

    /// Announce a tool call the model has decided on, in one shot.
    ///
    /// Default: emit `output_item.added` then `output_item.done` carrying the
    /// full call. A scheduler that streams arguments incrementally should
    /// instead call `output_item_added` → `function_call_arguments_delta`* →
    /// `function_call_arguments_done` → `output_item_done` itself and skip this.
    async fn tool_call(&mut self, call: &ToolCall) -> Result<SinkVerdict, SinkError> {
        let item = ResponseItem::FunctionCall {
            call_id: call.id.clone(),
            name: call.name.clone(),
            arguments: call.arguments.clone(),
            id: None,
            status: None,
        };
        if self.output_item_added(&item).await?.should_stop() {
            return Ok(SinkVerdict::Stop);
        }
        self.output_item_done(&item).await
    }
}

/// Whether the scheduler should keep going.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkVerdict {
    Continue,
    /// This attempt was superseded. Abandon it.
    Stop,
}

impl SinkVerdict {
    pub fn should_stop(self) -> bool {
        matches!(self, SinkVerdict::Stop)
    }
}

#[derive(Debug, Error)]
pub enum SchedulerError {
    /// The fence moved mid-generation; this attempt's output is void.
    #[error("attempt superseded")]
    Superseded,

    #[error("execution deadline exceeded")]
    DeadlineExceeded,

    /// Retrying may help: timeout, 5xx, connection reset.
    #[error("provider unavailable: {0}")]
    Unavailable(String),

    /// Retrying will not help: bad credentials, unknown model, malformed request.
    #[error("provider rejected the request: {0}")]
    Rejected(String),

    #[error("provider quota exhausted")]
    QuotaExhausted,

    /// Refused before dispatch, e.g. a queue at capacity. Distinct from
    /// `Unavailable`: nothing was sent, so no tokens were spent.
    #[error("scheduler refused the request: {0}")]
    Refused(String),

    #[error("scheduler produced no output items")]
    EmptyOutcome,

    #[error("scheduler produced a `{item_type}` item, which cannot be fed back as input")]
    UnusableOutput { item_type: &'static str },

    #[error("scheduler produced an invalid item: {0}")]
    InvalidOutput(String),

    #[error("could not deliver incremental output: {0}")]
    Sink(#[from] SinkError),

    #[error("{0}")]
    Other(String),
}

impl SchedulerError {
    /// Whether a fresh attempt could plausibly succeed.
    ///
    /// Matched exhaustively on purpose: adding a variant forces a decision here
    /// rather than silently defaulting to one answer.
    pub fn is_retryable(&self) -> bool {
        match self {
            SchedulerError::Unavailable(_)
            | SchedulerError::Refused(_)
            | SchedulerError::Sink(_) => true,
            SchedulerError::Superseded
            | SchedulerError::DeadlineExceeded
            | SchedulerError::Rejected(_)
            | SchedulerError::QuotaExhausted
            | SchedulerError::EmptyOutcome
            | SchedulerError::UnusableOutput { .. }
            | SchedulerError::InvalidOutput(_)
            | SchedulerError::Other(_) => false,
        }
    }
}

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("transport failure while streaming: {0}")]
    Transport(String),
    #[error("{0}")]
    Other(String),
}

/// Reject an outcome that cannot be stored, before it is submitted.
///
/// Lives with the port so every scheduler is held to the same standard, and so a
/// broken one fails at its own boundary where the error still names it.
pub fn validate_outcome(outcome: &CompletionsOutcome) -> Result<(), SchedulerError> {
    if outcome.items.is_empty() {
        return Err(SchedulerError::EmptyOutcome);
    }
    for item in &outcome.items {
        if !item.is_acceptable_as_input() {
            return Err(SchedulerError::UnusableOutput {
                item_type: item.item_type(),
            });
        }
        item.validate()
            .map_err(|e| SchedulerError::InvalidOutput(e.to_string()))?;
    }
    Ok(())
}

/// Collects everything pushed to it. For tests, and for schedulers that need to
/// inspect what they streamed.
#[derive(Debug, Default)]
pub struct CollectingSink {
    pub deltas: Vec<String>,
    pub calls: Vec<ToolCall>,
    /// Items announced via `output_item_added`, in order.
    pub added: Vec<ResponseItem>,
    /// Items announced via `output_item_done`, in order.
    pub done: Vec<ResponseItem>,
    /// Argument fragments announced via `function_call_arguments_delta`.
    pub arg_deltas: Vec<String>,
    /// Complete arguments announced via `function_call_arguments_done`.
    pub arg_dones: Vec<String>,
    /// Report a moved fence after this many deltas.
    pub stop_after: Option<usize>,
}

impl CollectingSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn stopping_after(n: usize) -> Self {
        Self {
            stop_after: Some(n),
            ..Default::default()
        }
    }

    /// Concatenation of everything streamed.
    ///
    /// For assertions only. Note the deliberate asymmetry with
    /// [`CompletionsOutcome::items`]: this is what the caller *saw*, which may
    /// legitimately differ from the final answer.
    pub fn streamed(&self) -> String {
        self.deltas.concat()
    }
}

#[async_trait]
impl CompletionsSink for CollectingSink {
    async fn text_delta(&mut self, text: &str) -> Result<SinkVerdict, SinkError> {
        self.deltas.push(text.to_string());
        match self.stop_after {
            Some(n) if self.deltas.len() >= n => Ok(SinkVerdict::Stop),
            _ => Ok(SinkVerdict::Continue),
        }
    }

    async fn tool_call(&mut self, call: &ToolCall) -> Result<SinkVerdict, SinkError> {
        self.calls.push(call.clone());
        Ok(SinkVerdict::Continue)
    }

    async fn output_item_added(&mut self, item: &ResponseItem) -> Result<SinkVerdict, SinkError> {
        self.added.push(item.clone());
        Ok(SinkVerdict::Continue)
    }

    async fn output_item_done(&mut self, item: &ResponseItem) -> Result<SinkVerdict, SinkError> {
        self.done.push(item.clone());
        Ok(SinkVerdict::Continue)
    }

    async fn function_call_arguments_delta(
        &mut self,
        _item_id: &str,
        delta: &str,
    ) -> Result<SinkVerdict, SinkError> {
        self.arg_deltas.push(delta.to_string());
        Ok(SinkVerdict::Continue)
    }

    async fn function_call_arguments_done(
        &mut self,
        _item_id: &str,
        arguments: &str,
    ) -> Result<SinkVerdict, SinkError> {
        self.arg_dones.push(arguments.to_string());
        Ok(SinkVerdict::Continue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ResponseItem, Role};
    use crate::Usage;

    #[test]
    fn a_plain_answer_is_storable() {
        validate_outcome(&CompletionsOutcome::text("hello", Usage::new(1, 1)))
            .expect("a plain answer must be storable");
    }

    #[test]
    fn an_empty_outcome_is_refused() {
        // Submitting nothing would complete the response with no content, which
        // reads to the caller as a successful empty answer.
        let o = CompletionsOutcome {
            items: vec![],
            usage: Usage::default(),
            finish: crate::completions::FinishReason::Stop,
        };
        assert!(matches!(
            validate_outcome(&o),
            Err(SchedulerError::EmptyOutcome)
        ));
    }

    #[test]
    fn an_invalid_item_is_refused_at_the_port_boundary() {
        // Caught here as well as at the gateway, so the failure names the scheduler
        // that produced it rather than surfacing as an opaque 400 later.
        let o = CompletionsOutcome {
            items: vec![ResponseItem::Message {
                role: Role::Assistant,
                content: vec![],
                id: None,
                status: None,
            }],
            usage: Usage::default(),
            finish: crate::completions::FinishReason::Stop,
        };
        assert!(matches!(
            validate_outcome(&o),
            Err(SchedulerError::InvalidOutput(_))
        ));
    }

    #[test]
    fn retryability_is_decided_per_variant() {
        assert!(SchedulerError::Unavailable("503".into()).is_retryable());
        // Refused before dispatch: nothing was sent, so a retry costs nothing extra.
        assert!(SchedulerError::Refused("queue full".into()).is_retryable());
        // Superseded must not retry: the work now belongs to another attempt.
        assert!(!SchedulerError::Superseded.is_retryable());
        // Nor may a rejection — the identical request would be rejected again.
        assert!(!SchedulerError::Rejected("unknown model".into()).is_retryable());
        assert!(!SchedulerError::QuotaExhausted.is_retryable());
    }

    #[tokio::test]
    async fn a_collecting_sink_can_simulate_a_moved_fence() {
        let mut sink = CollectingSink::stopping_after(2);
        assert_eq!(
            sink.text_delta("a").await.expect("push"),
            SinkVerdict::Continue
        );
        assert_eq!(
            sink.text_delta("b").await.expect("push"),
            SinkVerdict::Stop,
            "the sink must be able to tell a scheduler to abandon the attempt"
        );
        assert_eq!(sink.streamed(), "ab");
    }

    #[tokio::test]
    async fn tool_call_defaults_to_added_then_done() {
        // A sink that only implements the item-level methods must still see a
        // one-shot `tool_call`, because the default maps it to add then done.
        struct ItemOnlySink {
            added: Vec<ResponseItem>,
            done: Vec<ResponseItem>,
        }

        #[async_trait]
        impl CompletionsSink for ItemOnlySink {
            async fn text_delta(&mut self, _t: &str) -> Result<SinkVerdict, SinkError> {
                Ok(SinkVerdict::Continue)
            }
            async fn output_item_added(
                &mut self,
                item: &ResponseItem,
            ) -> Result<SinkVerdict, SinkError> {
                self.added.push(item.clone());
                Ok(SinkVerdict::Continue)
            }
            async fn output_item_done(
                &mut self,
                item: &ResponseItem,
            ) -> Result<SinkVerdict, SinkError> {
                self.done.push(item.clone());
                Ok(SinkVerdict::Continue)
            }
        }

        let mut sink = ItemOnlySink {
            added: vec![],
            done: vec![],
        };
        let call = ToolCall {
            id: "c1".into(),
            name: "f".into(),
            arguments: "{}".into(),
        };
        sink.tool_call(&call).await.expect("push");

        assert_eq!(sink.added.len(), 1);
        assert_eq!(sink.done.len(), 1);
        assert!(matches!(
            &sink.added[0],
            ResponseItem::FunctionCall { name, .. } if name == "f"
        ));
        assert_eq!(
            sink.added[0], sink.done[0],
            "add and done must carry the same item"
        );
    }
}
