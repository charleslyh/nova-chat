//! **Outbound** completions protocol — what we send to a model provider.
//!
//! # Direction matters, and it is the reason this module is separate
//!
//! `protocol/` is the **inbound** surface: the closed subset of the Responses
//! protocol that callers send *to us*, where every unknown field is a 400.
//!
//! This module is the **outbound** shape: the completions request we send *to a
//! provider*. The two are different protocols with different owners, and
//! conflating them would be a mistake in both directions — we would either start
//! accepting fields because a provider does, or start refusing to send fields
//! because our subset omits them.
//!
//! | module | direction | owner | violation means |
//! |---|---|---|---|
//! | `protocol/` | inbound | us (published subset) | reject with 400 |
//! | `completions/` | outbound | the provider | our request is malformed |
//!
//! # Why "completions" and not "chat"
//!
//! What actually goes over the wire is a completions request. "Chat" is a
//! higher-level notion — a conversation, its participants, its history — and the
//! service models that with responses and context chains, not with this type. A
//! `ChatRequest` would suggest this carries conversational semantics; it does not.
//! It carries one request/response exchange.
//!
//! # Inert data
//!
//! A [`CompletionsRequest`] holds no connections, clocks or callbacks. It
//! serialises losslessly, which is what makes a failing integration test
//! reproducible from its log alone: the request *is* the whole input.

mod outcome;
mod request;
mod translate;

pub use outcome::{assistant_text_message, CompletionsOutcome, FinishReason, ToolCall};
pub use request::{
    AssistantToolCall, CompletionsContent, CompletionsMessage, CompletionsRequest,
    RequestProvenance, ToolSpec,
};
pub use translate::{items_to_messages, TranslationError};
