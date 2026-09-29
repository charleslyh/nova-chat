//! Production executor: one model round, no tool loop.
//!
//! This crate is what turns a claimed response into a model round. It is the
//! service's own execution detail — the protocol says nothing about how a round
//! is performed, only what a round is — which is exactly why it lives behind
//! the [`nova_agent_runtime::AgentRunner`] port rather than in the protocol
//! crate.
//!
//! # Layout
//!
//! - [`catalog`] — request `model` → endpoint, credentials fail-closed.
//! - [`context`] — protocol items → completion messages.
//! - [`outcome`] — how a round ends, in protocol terms (the four-state split).
//! - [`runner`] — the port implementation: exactly one round.
//! - [`think`] — splitting a chain of thought a provider inlined in the body.
//! - [`transport`] — the chat-completions wire client.
//! - [`wire`] — the vocabulary the transport and the runner share.
//!
//! # What this crate deliberately does not do
//!
//! - **No tool execution.** Function tools belong to the caller; a round that
//!   asks for them ends `completed` with the calls as output.
//! - **No prompt composition.** Instructions arrive assembled, as a
//!   per-request protocol field.
//! - **No agent concepts.** No agent ids, no personas, no skills, no
//!   delegation. Keeping this tier ignorant of them is what makes it
//!   replaceable by an off-the-shelf model service.

pub mod catalog;
pub mod compact;
pub mod context;
pub mod outcome;
pub mod passthrough;
pub mod runner;
pub(crate) mod think;
pub mod transport;
pub mod wire;

pub use catalog::{CatalogError, ModelCatalog, ModelEntry};
pub use outcome::Termination;
pub use runner::{EnvReader, ProviderFactory, SingleTurnExecutor};
pub use transport::{ApiKey, HttpChatCompletion, ModelEndpoint};
pub use wire::{
    ChatCompletion, ChunkStream, FinishReason, ModelError, ModelUsage, RequestMessage,
    ResponseChunk, ToolCallRequest, ToolManifest, TransportOverrides,
};
