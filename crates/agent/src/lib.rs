//! The agent: run a response to a terminal state, in the node that created it.
//!
//! # What changed, and why the previous shape was wrong
//!
//! Execution used to be an external process polling `/v1/agent/claim`, then pushing
//! increments back over HTTP. That model rested on an assumption it never stated:
//! **the claiming node is the node holding the in-flight buffer.** The in-memory
//! backend satisfied it for free, since each node kept its own ledger.
//!
//! Once the ledger became shared (D21) the assumption failed silently. A worker
//! attached to node-a could claim node-b's response; its increments landed in
//! node-a's buffer while subscribers — routing by the node tag inside the id — were
//! sent to node-b, where they saw `Created` and then nothing at all. No error was
//! raised on any path.
//!
//! Executing inside the creating node makes the producer and the buffer holder the
//! same process **by construction** (D23), so no mechanism has to keep them
//! aligned.
//!
//! # A response *is* an agent
//!
//! A response is not one completions call: the model may call tools and answer
//! later, possibly across several rounds. [`Agent`] owns that loop — build a
//! request, schedule it, and either submit the answer or run the requested tools
//! and schedule again.
//!
//! ```text
//!   ledger / event log / context  ◀──  Agent  ── request ──▶  provider
//!            (ports, in-process)         │             (CompletionsRequestScheduler)
//!                                        └── tool call ──▶  ToolExecutor
//!                                        no HTTP anywhere in this crate
//! ```
//!
//! The agent owns *when* work runs and *how failures are classified*. A scheduler
//! owns everything about reaching a model — including queuing and throttling — and
//! a [`ToolExecutor`] owns everything about carrying a tool call out. Keeping both
//! behind ports means changing provider or tooling does not mean re-tuning the
//! fleet or touching this loop.
//!
//! # Testable without a socket or a model
//!
//! The agent takes ports, so the in-memory adapters plus mock ports exercise the
//! full path in a unit test: claim, multi-round loop, submit, abandon on a moved
//! fence, and fail loudly on an unusable outcome or a tool error.

pub mod engine;

pub use engine::{Agent, AgentConfig, AgentDeps, Executed};
