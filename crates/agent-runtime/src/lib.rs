//! The agent execution runtime: an orchestrator over a pluggable [`AgentRunner`].
//!
//! # Responsibilities split
//!
//! - [`AgentRuntime`] owns the orchestration: claim from the ledger, assemble the
//!   task, hand it to a runner, then commit the result (complete / fail) and the
//!   conversation bookkeeping.
//! - [`AgentRunner`] owns the ReAct loop: how a task is actually executed (mock
//!   provider, real agent SDK, …) is an implementation detail behind this trait.
//! - [`EventSink`] owns streaming: a runner pushes incremental events through it,
//!   and it appends them to the shared event log.
//!
//! The crate stays IO-free: it depends only on `nova-responses-core` ports, so
//! the whole claim/stream/commit path is testable without a socket or a model.

mod runner;
mod runtime;
mod sink;

pub use runner::{
    AgentError, AgentEventSink, AgentOutcome, AgentRunner, AgentTask, CancelProbe, SinkError,
    SinkVerdict,
};
pub use runtime::{
    AgentRuntime, AgentRuntimeConfig, AgentRuntimeDeps, AgentRuntimeHandle, Executed,
};
pub use sink::EventSink;
