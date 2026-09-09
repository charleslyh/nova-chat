//! The agent execution abstraction: [`AgentRunner`] plus the task, outcome and
//! event-sink types it traffics in.
//!
//! # Why this lives here, not in core
//!
//! `nova-responses-core` is the storage/domain/protocol contract. This crate is
//! the agent layer: [`AgentRunner`] is the seam between the orchestrator
//! ([`crate::AgentRuntime`]) and whatever actually runs the ReAct loop (a mock
//! provider, a real agent SDK, …). Keeping the trait here lets each runner
//! implementation depend on the agent layer without dragging the whole domain
//! into every provider adapter.

use async_trait::async_trait;
use nova_responses::protocol::{Tool, ToolChoice};
use nova_responses::{RequestProvenance, ResponseItem, ResponseStatus, Usage};

/// One agent execution task, assembled by the orchestrator after a claim.
#[derive(Debug, Clone)]
pub struct AgentTask {
    pub model: String,
    pub instructions: Option<String>,
    /// Functions offered to the model this turn, in inbound protocol shape
    /// (the caller's declaration); provider translation is the runner's job.
    pub tools: Vec<Tool>,
    pub tool_choice: Option<ToolChoice>,
    /// Initial conversation (`snapshot.items + input_items`, D30).
    pub items: Vec<ResponseItem>,
    pub provenance: RequestProvenance,
    pub max_tool_rounds: usize,
}

/// The final product of one agent execution.
#[derive(Debug, Clone)]
pub struct AgentOutcome {
    /// Everything produced this turn (tool call + output + answer), in order.
    pub items: Vec<ResponseItem>,
    pub usage: Usage,
    /// `Completed` or `Incomplete`.
    pub status: ResponseStatus,
}

/// Why an agent run stopped.
#[derive(Debug, thiserror::Error)]
pub enum AgentError {
    /// The attempt fence moved (reap or cancel): this work is void.
    #[error("attempt superseded")]
    Superseded,
    /// The runner failed; `usage` is what was consumed before the failure, so
    /// the terminal transition still books it (INV-51).
    #[error("{message}")]
    Failed { message: String, usage: Usage },
}

/// Whether the stream should continue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkVerdict {
    Continue,
    Stop,
}

impl SinkVerdict {
    pub fn should_stop(self) -> bool {
        matches!(self, SinkVerdict::Stop)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error("transport failure while streaming: {0}")]
    Transport(String),
    #[error("{0}")]
    Other(String),
}

/// Event sink: the runner pushes incremental output through it, and the
/// orchestrator's [`crate::EventSink`] appends each event to the event log.
///
/// A returned [`SinkVerdict::Stop`] means the attempt fence moved — the caller
/// must stop immediately and not produce further output for this attempt.
#[async_trait]
pub trait AgentEventSink: Send {
    async fn text_delta(&mut self, text: &str) -> Result<SinkVerdict, SinkError>;

    async fn reasoning_text_delta(&mut self, _text: &str) -> Result<SinkVerdict, SinkError> {
        Ok(SinkVerdict::Continue)
    }

    async fn output_item_added(&mut self, _item: &ResponseItem) -> Result<SinkVerdict, SinkError> {
        Ok(SinkVerdict::Continue)
    }

    async fn output_item_done(&mut self, _item: &ResponseItem) -> Result<SinkVerdict, SinkError> {
        Ok(SinkVerdict::Continue)
    }

    async fn function_call_arguments_delta(
        &mut self,
        _item_id: &str,
        _delta: &str,
    ) -> Result<SinkVerdict, SinkError> {
        Ok(SinkVerdict::Continue)
    }

    async fn function_call_arguments_done(
        &mut self,
        _item_id: &str,
        _arguments: &str,
    ) -> Result<SinkVerdict, SinkError> {
        Ok(SinkVerdict::Continue)
    }

    async fn content_part_added(
        &mut self,
        _item_id: &str,
        _content_index: u32,
    ) -> Result<SinkVerdict, SinkError> {
        Ok(SinkVerdict::Continue)
    }

    async fn output_text_done(&mut self, _text: &str) -> Result<SinkVerdict, SinkError> {
        Ok(SinkVerdict::Continue)
    }

    async fn content_part_done(
        &mut self,
        _item_id: &str,
        _content_index: u32,
        _text: &str,
    ) -> Result<SinkVerdict, SinkError> {
        Ok(SinkVerdict::Continue)
    }

    /// Announce a whole tool call in one shot, mapped to `added` then `done`.
    async fn tool_call(
        &mut self,
        call_id: &str,
        name: &str,
        arguments: &str,
    ) -> Result<SinkVerdict, SinkError> {
        let item = ResponseItem::FunctionCall {
            call_id: call_id.to_string(),
            name: name.to_string(),
            arguments: arguments.to_string(),
            id: None,
            status: None,
        };
        if self.output_item_added(&item).await?.should_stop() {
            return Ok(SinkVerdict::Stop);
        }
        self.output_item_done(&item).await
    }
}

/// Executes an agent task; the ReAct loop lives in the implementation.
#[async_trait]
pub trait AgentRunner: Send + Sync {
    fn name(&self) -> &str;

    /// Run `task` to completion. Incremental events are pushed through `sink`
    /// (whose implementation appends them to the event log); when the sink
    /// returns [`SinkVerdict::Stop`] the implementation must stop and return
    /// [`AgentError::Superseded`].
    async fn run(
        &self,
        task: &AgentTask,
        sink: &mut dyn AgentEventSink,
    ) -> Result<AgentOutcome, AgentError>;
}
