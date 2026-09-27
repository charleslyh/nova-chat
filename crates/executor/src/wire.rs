//! The chat-completions vocabulary this executor speaks to a model.
//!
//! Ported from the retiring `nova-agent` kernel, which is where these types
//! were proven. Two deliberate changes were made while moving them:
//!
//! - **Tool parameters stay a `Value`.** The original carried the JSON Schema
//!   as a `String`, which forced the transport to re-parse it with `.ok()` —
//!   a schema that failed to parse was silently sent as "no parameters". The
//!   protocol side already holds a `Value`, so keeping it typed end to end
//!   removes both the round-trip and the silent drop.
//! - **Errors are local.** No dependency on the retiring crate's error type.

use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A model turn failed. The message is for the operator; it is mapped to a
/// protocol failure by the caller.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ModelError(pub String);

impl ModelError {
    pub fn new(message: impl std::fmt::Display) -> Self {
        Self(message.to_string())
    }
}

/// A function offered to the model this turn.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolManifest {
    pub name: String,
    pub description: String,
    /// JSON Schema object, passed through untouched.
    pub parameters: Value,
}

/// A tool call the model asked for.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolCallRequest {
    pub call_id: String,
    pub name: String,
    pub arguments: Value,
}

/// One message in the completion context.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "lowercase")]
pub enum RequestMessage {
    System {
        content: String,
    },
    User {
        content: String,
    },
    Assistant {
        content: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tool_calls: Option<Vec<ToolCallRequest>>,
    },
    Tool {
        content: String,
        call_id: String,
    },
}

/// Why the model stopped generating this round.
///
/// A **semantic** summary, not a wire mirror. In particular there is no
/// `ToolCalls` variant: a round that stops for tools already says so by
/// emitting [`ResponseChunk::ToolCall`] items before [`ResponseChunk::Done`],
/// and the wire's `finish_reason: "tool_calls"` maps to [`Self::Stop`].
///
/// Reading this enum to decide "did this round request tools" is therefore a
/// bug; count the `ToolCall` chunks instead.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    /// Natural end of the round (includes the wire's `stop` and `tool_calls`).
    Stop,
    /// Hit the requested or model output budget.
    Length,
    /// Provider declined or withheld output; `reason` carries explicit refusal
    /// wording when the adapter merged any.
    Refusal {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

/// Token accounting for one model round.
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ModelUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
    #[serde(default)]
    pub cached_tokens: u32,
    #[serde(default)]
    pub reasoning_tokens: u32,
}

/// One item from a [`ChatCompletion::completion`] stream.
///
/// Canonical order for one round:
///
/// 1. zero or more [`Self::TextBlock`] and/or [`Self::Think`]
/// 2. optional [`Self::ThinkDone`] after the last [`Self::Think`]
/// 3. optional [`Self::TextDone`] after the last [`Self::TextBlock`] — adapters
///    MUST synthesise it when the round also emits tool calls
/// 4. zero or more [`Self::ToolCall`] (finalised, stable order)
/// 5. exactly one [`Self::Done`]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "content", rename_all = "snake_case")]
pub enum ResponseChunk {
    TextBlock(String),
    Think(String),
    ThinkDone,
    TextDone,
    ToolCall(ToolCallRequest),
    Done {
        reason: FinishReason,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage: Option<ModelUsage>,
    },
}

/// A streaming completion provider.
///
/// Kept as a trait for one reason: tests script a chunk sequence instead of
/// standing up an HTTP endpoint. Every behaviour in this crate above the wire
/// is therefore testable without a model.
#[async_trait]
pub trait ChatCompletion: Send + Sync {
    async fn completion(
        &self,
        messages: &[RequestMessage],
        tools: &[ToolManifest],
        stream: bool,
    ) -> Result<ChunkStream, ModelError>;
}

/// The stream a provider returns.
pub type ChunkStream = Pin<Box<dyn Stream<Item = Result<ResponseChunk, ModelError>> + Send>>;
