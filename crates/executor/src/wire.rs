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

use crate::runner::StreamTimeouts;

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
    /// The caller's `strict` flag. Reaches the wire only when the endpoint
    /// opts in, since constrained decoding is not safe for every tool format.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strict: Option<bool>,
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
        /// The thinking this turn was produced with. Interleaved-thinking models
        /// expect it back on every assistant turn of the current tool loop.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reasoning: Option<String>,
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
    /// The stream ended without the upstream saying why. A gateway that times
    /// out closes the connection mid-round, so this is a cut-off, never a stop:
    /// calling it `Stop` once committed half-written tool arguments as a
    /// finished round.
    Interrupted,
    /// The upstream said it stopped to call tools (`finish_reason:
    /// "tool_calls"`) but no call survived its parser. Seen when the model's
    /// tool-call markup is malformed: the server keeps the text before it and
    /// drops the call.
    MissingToolCalls,
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

/// What one round must carry to the upstream beyond its static endpoint config.
///
/// A proxied model needs an accounting id (and sometimes a caller-chosen key)
/// that no endpoint declaration can express, so the host stamps it per round
/// and this crate rides it along. Deliberately opaque: the transport does not
/// know what a billing id or a credential MEANS, only that this round asked
/// for a set of headers.
///
/// Values are never logged — the host may put a credential in one.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TransportOverrides {
    headers: std::collections::BTreeMap<String, String>,
}

impl TransportOverrides {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }

    /// Stamp one header, replacing any value already held under `name`.
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    /// The stamped headers, in name order.
    pub fn headers(&self) -> impl Iterator<Item = (&str, &str)> {
        self.headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }
}

/// A streaming completion provider.
///
/// Kept as a trait for one reason: tests script a chunk sequence instead of
/// standing up an HTTP endpoint. Every behaviour in this crate above the wire
/// is therefore testable without a model.
#[async_trait]
pub trait ChatCompletion: Send + Sync {
    /// Perform one round. `transport` carries this round's host-stamped
    /// headers; a round talking to an internal endpoint passes an empty set.
    async fn completion(
        &self,
        messages: &[RequestMessage],
        tools: &[ToolManifest],
        stream: bool,
        transport: &TransportOverrides,
    ) -> Result<ChunkStream, ModelError>;

    /// How long this endpoint's rounds may go quiet, when it names its own.
    /// `None` leaves the executor's defaults in force: a queueing upstream and
    /// a local one do not wait alike, so the endpoint that knows best says.
    fn stream_timeouts(&self) -> Option<StreamTimeouts> {
        None
    }
}

/// The stream a provider returns.
pub type ChunkStream = Pin<Box<dyn Stream<Item = Result<ResponseChunk, ModelError>> + Send>>;
