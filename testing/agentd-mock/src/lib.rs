//! The mock verification process's agent library.
//!
//! Provides the first [`nova_agent_runtime::AgentRunner`] implementation — a
//! ReAct loop over a completions scheduler (mock or HTTP) plus a tool executor —
//! and the completions/tool types it needs. This is a verification fixture, not
//! a reusable production adapter.

pub mod completions;
pub mod mock_runner;
pub mod scheduler;
pub mod tool;

pub use mock_runner::MockAgentRunner;
pub use scheduler::{
    EchoScheduler, HttpChatCompletionsScheduler, Match, Script, ScriptRule, Scheduler,
    SchedulerError, ScriptedScheduler,
};
pub use tool::{CalculatorTool, NoopToolExecutor, ToolError, ToolExecutor, TOOL_NAME};
