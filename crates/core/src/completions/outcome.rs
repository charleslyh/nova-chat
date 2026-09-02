//! What a scheduled completions request produced.

use crate::protocol::{ContentPart, ResponseItem, Role};
use crate::Usage;

/// Why generation ended.
///
/// Distinct from success or failure: a truncated answer succeeded at the transport
/// level and still must not be treated as complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinishReason {
    Stop,
    /// Hit the output ceiling. The answer is partial.
    Length,
    ToolCalls,
    /// The model declined.
    Refusal,
}

impl FinishReason {
    /// Whether the answer can be treated as a complete reply.
    ///
    /// `Length` is deliberately **not** complete: storing a truncated answer as if
    /// it were whole silently corrupts every later turn that builds on it.
    pub fn is_complete_answer(self) -> bool {
        matches!(self, FinishReason::Stop | FinishReason::ToolCalls)
    }
}

/// A tool call announced mid-stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: String,
}

/// The result of one completions request, in **our** vocabulary.
///
/// The conversion back from provider shape happens inside the scheduler, so
/// nothing downstream ever sees a provider type. That is what keeps provider
/// coupling from spreading past the adapter.
#[derive(Debug, Clone, PartialEq)]
pub struct CompletionsOutcome {
    /// The final items, submitted explicitly.
    ///
    /// Never reconstructed from streamed deltas by the caller: the delta stream is
    /// a bounded transient buffer, and durable history may not depend on it
    /// (FR-20 / INV-48).
    pub items: Vec<ResponseItem>,
    pub usage: Usage,
    pub finish: FinishReason,
}

impl CompletionsOutcome {
    /// A plain text answer.
    pub fn text(text: impl Into<String>, usage: Usage) -> Self {
        Self {
            items: vec![ResponseItem::Message {
                role: Role::Assistant,
                content: vec![ContentPart::OutputText { text: text.into() }],
                id: None,
                status: None,
            }],
            usage,
            finish: FinishReason::Stop,
        }
    }

    /// A refusal.
    ///
    /// Separate constructor because a refusal is a legitimate *completed* turn, not
    /// an error, and must be storable as such — treating it as a failure would
    /// leave the response non-terminal and the caller waiting.
    pub fn refusal(reason: impl Into<String>, usage: Usage) -> Self {
        Self {
            items: vec![ResponseItem::Message {
                role: Role::Assistant,
                content: vec![ContentPart::Refusal {
                    refusal: reason.into(),
                }],
                id: None,
                status: None,
            }],
            usage,
            finish: FinishReason::Refusal,
        }
    }

    /// A tool-call turn.
    pub fn tool_calls(calls: Vec<ToolCall>, usage: Usage) -> Self {
        Self {
            items: calls
                .into_iter()
                .map(|c| ResponseItem::FunctionCall {
                    call_id: c.id,
                    name: c.name,
                    arguments: c.arguments,
                    id: None,
                    status: None,
                })
                .collect(),
            usage,
            finish: FinishReason::ToolCalls,
        }
    }

    /// Whether every item could be fed back as input next turn (CR-12 / INV-47).
    pub fn is_chain_closed(&self) -> bool {
        self.items.iter().all(ResponseItem::is_acceptable_as_input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncation_is_not_a_complete_answer() {
        // The distinction that matters: `Length` succeeded at the transport level.
        // Storing it as complete would corrupt every turn that builds on it.
        assert!(!FinishReason::Length.is_complete_answer());
        assert!(FinishReason::Stop.is_complete_answer());
        assert!(FinishReason::ToolCalls.is_complete_answer());
    }

    #[test]
    fn every_constructor_yields_chain_closed_items() {
        // Our own context chain breaks with no external caller involved if an
        // outcome carries an item we cannot accept as input.
        assert!(CompletionsOutcome::text("hi", Usage::default()).is_chain_closed());
        assert!(CompletionsOutcome::refusal("no", Usage::default()).is_chain_closed());
        assert!(CompletionsOutcome::tool_calls(
            vec![ToolCall {
                id: "c".into(),
                name: "f".into(),
                arguments: "{}".into(),
            }],
            Usage::default()
        )
        .is_chain_closed());
    }

    #[test]
    fn a_refusal_is_a_completed_turn() {
        let o = CompletionsOutcome::refusal("cannot help", Usage::new(1, 0));
        assert_eq!(o.finish, FinishReason::Refusal);
        assert!(!o.finish.is_complete_answer());
        // Still storable: the caller must be able to read why it was refused.
        assert!(o.is_chain_closed());
        assert_eq!(o.items.len(), 1);
    }
}
