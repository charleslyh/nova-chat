//! The mock runner's outbound scheduler: reaches a model provider (mock or
//! real) behind a trait local to this crate.
//!
//! This trait and the [`SchedulerError`]/[`validate_outcome`] helpers were
//! previously `nova-responses-core::CompletionsRequestScheduler`. They are not
//! part of the storage/domain contract — only the mock agent runner uses them —
//! so they live here, and the trait's sink is the agent layer's
//! [`nova_agent_runtime::AgentEventSink`].

mod echo;
mod http;
mod scripted;

use async_trait::async_trait;
use thiserror::Error;

use nova_agent_runtime::{AgentEventSink, SinkError};
use nova_responses::protocol::ProtocolLimits;

use crate::completions::{CompletionsOutcome, CompletionsRequest};

pub use echo::EchoScheduler;
pub use http::HttpChatCompletionsScheduler;
pub use scripted::{Match, Script, ScriptRule, ScriptedScheduler};

#[derive(Debug, Error)]
pub enum SchedulerError {
    /// The fence moved mid-generation; this attempt's output is void.
    #[error("attempt superseded")]
    Superseded,
    #[error("execution deadline exceeded")]
    DeadlineExceeded,
    #[error("provider unavailable: {0}")]
    Unavailable(String),
    #[error("provider rejected the request: {0}")]
    Rejected(String),
    #[error("provider quota exhausted")]
    QuotaExhausted,
    #[error("scheduler refused the request: {0}")]
    Refused(String),
    #[error("scheduler produced no output items")]
    EmptyOutcome,
    #[error("scheduler produced a `{item_type}` item, which cannot be fed back as input")]
    UnusableOutput { item_type: String },
    #[error("scheduler produced an invalid item: {0}")]
    InvalidOutput(String),
    #[error("could not deliver incremental output: {0}")]
    Sink(#[from] SinkError),
    #[error("{0}")]
    Other(String),
}

impl SchedulerError {
    /// Whether a fresh attempt could plausibly succeed.
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

/// Reject an outcome that cannot be stored, before it is submitted.
///
/// `limits` are the same bounds the ingress applies to caller input: our own output has to
/// clear them too, or the chain would break on the next turn without any external caller
/// being involved (INV-47).
pub fn validate_outcome(
    outcome: &CompletionsOutcome,
    limits: &ProtocolLimits,
) -> Result<(), SchedulerError> {
    if outcome.items.is_empty() {
        return Err(SchedulerError::EmptyOutcome);
    }
    for item in &outcome.items {
        if !item.is_acceptable_as_input() {
            return Err(SchedulerError::UnusableOutput {
                item_type: item.item_type().to_string(),
            });
        }
        item.validate(limits)
            .map_err(|e| SchedulerError::InvalidOutput(e.to_string()))?;
    }
    Ok(())
}

/// Schedules and fulfils completions requests.
#[async_trait]
pub trait Scheduler: Send + Sync {
    fn name(&self) -> &str;

    async fn schedule(
        &self,
        request: &CompletionsRequest,
        sink: &mut dyn AgentEventSink,
    ) -> Result<CompletionsOutcome, SchedulerError>;
}

/// Split text into roughly `n` pieces, preserving content exactly.
pub(crate) fn chunk_text(text: &str, n: usize) -> Vec<String> {
    if n <= 1 || text.is_empty() {
        return vec![text.to_string()];
    }
    let chars: Vec<char> = text.chars().collect();
    let size = chars.len().div_ceil(n).max(1);
    chars
        .chunks(size)
        .map(|c| c.iter().collect::<String>())
        .collect()
}
