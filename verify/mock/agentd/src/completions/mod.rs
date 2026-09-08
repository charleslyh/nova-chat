//! Outbound completions protocol, vendored into the mock verification process.
//!
//! These types were previously `nova-responses-core::completions`. They are the
//! *outbound* shape — what we send to a model provider — and are now used only
//! by the mock agent runner in this crate. The inbound protocol subset stays in
//! `nova-responses-core::protocol`.

mod outcome;
mod request;
mod translate;

pub use outcome::{assistant_text_message, CompletionsOutcome, FinishReason, ToolCall};
pub use request::{
    AssistantToolCall, CompletionsContent, CompletionsMessage, CompletionsRequest,
    CompletionsToolChoice, SpecificFunction, ToolSpec,
};
pub use translate::{items_to_messages, TranslationError};
