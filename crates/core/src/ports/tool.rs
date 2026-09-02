//! Tool execution port.
//!
//! # Declared separately from the `ToolSpec`s handed to the model
//!
//! A [`ToolSpec`] is *what the model may call*; this port is *what carries the
//! call out*. Keeping the two apart means the agent loop neither knows nor cares
//! how a tool is implemented — a registry of pure functions and a remote MCP
//! bridge are the same shape behind this trait.
//!
//! # Direction: outbound, like the scheduler
//!
//! The agent loop calls this port after the scheduler reports
//! [`FinishReason::ToolCalls`]. It is the second of the two outbound edges, and
//! for the same reason the scheduler is a port: a tool that hits a database or a
//! remote service must stay swappable without touching the loop that drives it.

use async_trait::async_trait;
use thiserror::Error;

/// Why a tool call could not be carried out.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ToolError {
    /// The model named a tool the executor does not hold.
    ///
    /// Distinct from [`ToolError::Execution`]: this is a contract violation on
    /// the model's side (or a misconfigured spec list), not a runtime failure.
    #[error("no tool named `{0}` is available")]
    UnknownTool(String),

    /// The tool exists but failed while running.
    #[error("tool `{name}` failed: {message}")]
    Execution { name: String, message: String },
}

/// Executes the tools the agent offers to the model.
///
/// Implementations must be `Send + Sync` so one executor can serve concurrent
/// agents; per-call state belongs in locals, not in `self`.
#[async_trait]
pub trait ToolExecutor: Send + Sync {
    /// Stable identifier for logs and metrics, e.g. `"registry"`, `"mcp"`.
    fn name(&self) -> &str;

    /// Run `tool` with the JSON `arguments` the model supplied.
    ///
    /// The result is returned as an opaque string and fed back to the model as a
    /// `function_call_output` item. `Err` **terminates the response** — a tool
    /// that wants the model to see a recoverable failure should return it as `Ok`
    /// text instead, so the model can react (retry, or answer differently).
    async fn call(&self, tool: &str, arguments: &str) -> Result<String, ToolError>;
}

/// The "no tools are configured" executor.
///
/// Always refuses, so a model that nevertheless emits a tool call fails loudly
/// instead of hanging or being silently dropped. This is the explicit expression
/// of "the agent offers no tools", passed in place of a real executor.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopToolExecutor;

#[async_trait]
impl ToolExecutor for NoopToolExecutor {
    fn name(&self) -> &str {
        "none"
    }

    async fn call(&self, tool: &str, _arguments: &str) -> Result<String, ToolError> {
        Err(ToolError::UnknownTool(tool.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_noop_executor_refuses_every_call() {
        let e = NoopToolExecutor;
        assert_eq!(
            e.call("anything", "{}").await,
            Err(ToolError::UnknownTool("anything".into()))
        );
    }
}
