//! What a scheduled completions request produced.

use nova_responses_core::{ContentPart, ResponseItem, Role, Usage};

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
#[derive(Debug, Clone, PartialEq)]
pub struct CompletionsOutcome {
    /// The final items, submitted explicitly.
    pub items: Vec<ResponseItem>,
    pub usage: Usage,
    pub finish: FinishReason,
}

/// An assistant text message with a freshly generated item id.
pub fn assistant_text_message(text: impl Into<String>) -> ResponseItem {
    let text = text.into();
    ResponseItem::Message {
        role: Role::Assistant,
        content: vec![ContentPart::OutputText { text: text.clone() }],
        id: Some(format!("msg_{:x}", stable_id(&text))),
        status: None,
    }
}

/// A content-derived id, stable across runs, so a deterministic scheduler stays
/// deterministic (the CI-oracle property) without a random source.
fn stable_id(text: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in text.bytes() {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

impl CompletionsOutcome {
    /// A plain text answer.
    pub fn text(text: impl Into<String>, usage: Usage) -> Self {
        Self {
            items: vec![assistant_text_message(text)],
            usage,
            finish: FinishReason::Stop,
        }
    }

    /// A refusal.
    pub fn refusal(reason: impl Into<String>, usage: Usage) -> Self {
        let reason = reason.into();
        Self {
            items: vec![ResponseItem::Message {
                role: Role::Assistant,
                content: vec![ContentPart::Refusal {
                    refusal: reason.clone(),
                }],
                id: Some(format!("msg_{:x}", stable_id(&reason))),
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
        assert!(!FinishReason::Length.is_complete_answer());
        assert!(FinishReason::Stop.is_complete_answer());
        assert!(FinishReason::ToolCalls.is_complete_answer());
    }

    #[test]
    fn every_constructor_yields_chain_closed_items() {
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
        assert!(o.is_chain_closed());
        assert_eq!(o.items.len(), 1);
    }
}
