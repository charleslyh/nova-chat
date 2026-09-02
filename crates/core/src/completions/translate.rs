//! `ResponseItem` → `CompletionsMessage`.
//!
//! Done once, here, rather than in every scheduler. The mapping is not mechanical:
//! the completions protocol and our item set disagree in two ways that matter.
//!
//! **1. Tool results have their own role.** We carry a `FunctionCallOutput` item;
//! completions wants a `tool` message keyed by `tool_call_id`.
//!
//! **2. Parallel calls group onto one message.** We emit one `FunctionCall` item
//! per call. Completions expects a *single* assistant message carrying all of them
//! in `tool_calls`. Emitting one assistant message per call describes a different
//! conversation — the model reads it as several sequential decisions rather than
//! one parallel batch — and providers may reject it outright when the following
//! `tool` messages cannot be matched up.
//!
//! Getting this wrong degrades answer quality **without producing an error**,
//! which is why it is isolated behind a tested function instead of being
//! open-coded per provider.
//!
//! It lives in `core` rather than beside a scheduler because every scheduler needs
//! it and none of them owns it.

use thiserror::Error;

use super::request::{
    AssistantToolCall, CompletionsContent, CompletionsMessage, ImageUrlPayload,
};
use crate::protocol::{ContentPart, ResponseItem, Role};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TranslationError {
    /// A tool result with no preceding call. Refused rather than dropped: the
    /// provider would reject the unmatched `tool_call_id`, and a silently dropped
    /// result means the model never learns what its call returned.
    #[error("tool output for call `{call_id}` has no preceding call in this context")]
    OrphanToolOutput { call_id: String },

    #[error("context contains no messages to send")]
    EmptyContext,
}

/// Convert a resolved item sequence into provider messages.
pub fn items_to_messages(items: &[ResponseItem]) -> Result<Vec<CompletionsMessage>, TranslationError> {
    let mut out: Vec<CompletionsMessage> = Vec::with_capacity(items.len());
    // Call ids seen so far, to catch an unmatched tool result.
    let mut known_calls: Vec<String> = Vec::new();

    for item in items {
        match item {
            ResponseItem::Message {
                role,
                content,
                ..
            } => match role {
                Role::System | Role::Developer => out.push(CompletionsMessage::System {
                    content: flatten_text(content),
                }),
                Role::User => out.push(CompletionsMessage::User {
                    content: content.iter().filter_map(content_part).collect(),
                }),
                Role::Assistant => {
                    let refusal = content.iter().find_map(|p| match p {
                        ContentPart::Refusal { refusal } => Some(refusal.clone()),
                        _ => None,
                    });
                    out.push(CompletionsMessage::Assistant {
                        content: Some(flatten_text(content)),
                        refusal,
                        tool_calls: Vec::new(),
                    });
                }
            },

            ResponseItem::FunctionCall {
                call_id,
                name,
                arguments,
                ..
            } => {
                known_calls.push(call_id.clone());
                let call = AssistantToolCall::new(
                    call_id.clone(),
                    name.clone(),
                    arguments.clone(),
                );
                // Fold onto the previous assistant message when it is a
                // tool-call-only message, so parallel calls arrive as one batch.
                match out.last_mut() {
                    Some(CompletionsMessage::Assistant {
                        content,
                        tool_calls,
                        ..
                    }) if content.is_none() || tool_calls.is_empty() && content.is_none() => {
                        tool_calls.push(call);
                    }
                    _ => out.push(CompletionsMessage::Assistant {
                        content: None,
                        refusal: None,
                        tool_calls: vec![call],
                    }),
                }
            }

            ResponseItem::FunctionCallOutput {
                call_id, output, ..
            } => {
                if !known_calls.contains(call_id) {
                    return Err(TranslationError::OrphanToolOutput {
                        call_id: call_id.clone(),
                    });
                }
                out.push(CompletionsMessage::Tool {
                    tool_call_id: call_id.clone(),
                    content: output.clone(),
                });
            }
        }
    }

    Ok(out)
}

/// Concatenate the textual parts of an item.
///
/// Non-text parts are dropped here because assistant and system messages are
/// plain strings in the provider shape; user messages keep their parts and go
/// through [`content_part`] instead.
fn flatten_text(parts: &[ContentPart]) -> String {
    let mut s = String::new();
    for p in parts {
        match p {
            ContentPart::InputText { text } | ContentPart::OutputText { text } => s.push_str(text),
            ContentPart::InputImage { .. } | ContentPart::InputFile { .. } | ContentPart::Refusal { .. } => {}
        }
    }
    s
}

fn content_part(p: &ContentPart) -> Option<CompletionsContent> {
    match p {
        ContentPart::InputText { text } | ContentPart::OutputText { text } => {
            Some(CompletionsContent::Text { text: text.clone() })
        }
        // Refusal is an assistant-message-level field, not a content part.
        ContentPart::Refusal { .. } => None,
        // Reference forms only; there is no inline-bytes variant to handle.
        ContentPart::InputImage {
            image_url, file_id, ..
        } => match (image_url, file_id) {
            (Some(url), _) => Some(CompletionsContent::ImageUrl {
                image_url: ImageUrlPayload { url: url.clone() },
            }),
            (None, Some(id)) => Some(CompletionsContent::FileRef { id: id.clone() }),
            (None, None) => None,
        },
        ContentPart::InputFile {
            file_id, file_url, ..
        } => match (file_id, file_url) {
            (Some(id), _) => Some(CompletionsContent::FileRef { id: id.clone() }),
            (None, Some(url)) => Some(CompletionsContent::ImageUrl {
                image_url: ImageUrlPayload { url: url.clone() },
            }),
            (None, None) => None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(text: &str) -> ResponseItem {
        ResponseItem::Message {
            role: Role::User,
            content: vec![ContentPart::InputText { text: text.into() }],
            id: None,
            status: None,
        }
    }

    fn assistant(text: &str) -> ResponseItem {
        ResponseItem::Message {
            role: Role::Assistant,
            content: vec![ContentPart::OutputText { text: text.into() }],
            id: None,
            status: None,
        }
    }

    fn call(id: &str, name: &str) -> ResponseItem {
        ResponseItem::FunctionCall {
            call_id: id.into(),
            name: name.into(),
            arguments: "{}".into(),
            id: None,
            status: None,
        }
    }

    fn call_output(id: &str, out: &str) -> ResponseItem {
        ResponseItem::FunctionCallOutput {
            call_id: id.into(),
            output: out.into(),
            id: None,
            status: None,
        }
    }

    #[test]
    fn chronological_order_is_preserved() {
        let msgs = items_to_messages(&[user("q1"), assistant("a1"), user("q2")])
            .expect("translate");
        let roles: Vec<_> = msgs.iter().map(CompletionsMessage::role_name).collect();
        assert_eq!(roles, vec!["user", "assistant", "user"]);
    }

    #[test]
    fn parallel_tool_calls_become_one_assistant_message() {
        // The decision this module exists for. One assistant message per call
        // describes sequential decisions rather than a parallel batch, and leaves
        // the following tool messages unmatchable.
        let msgs = items_to_messages(&[
            user("look these up"),
            call("c1", "lookup"),
            call("c2", "lookup"),
            call_output("c1", "r1"),
            call_output("c2", "r2"),
        ])
        .expect("translate");

        let roles: Vec<_> = msgs.iter().map(CompletionsMessage::role_name).collect();
        assert_eq!(
            roles,
            vec!["user", "assistant", "tool", "tool"],
            "both calls must fold onto a single assistant message"
        );
        match &msgs[1] {
            CompletionsMessage::Assistant { tool_calls, .. } => {
                assert_eq!(tool_calls.len(), 2);
                assert_eq!(tool_calls[0].id, "c1");
                assert_eq!(tool_calls[1].id, "c2");
            }
            other => panic!("expected assistant, got {other:?}"),
        }
    }

    #[test]
    fn a_call_after_assistant_text_starts_a_new_message() {
        // Text and calls in one message would misrepresent the turn: the model
        // spoke, then decided to call. Folding them would claim it did both at once.
        let msgs = items_to_messages(&[user("q"), assistant("thinking"), call("c1", "f")])
            .expect("translate");
        let roles: Vec<_> = msgs.iter().map(CompletionsMessage::role_name).collect();
        assert_eq!(roles, vec!["user", "assistant", "assistant"]);
        match &msgs[2] {
            CompletionsMessage::Assistant {
                content,
                tool_calls,
                ..
            } => {
                assert!(content.is_none());
                assert_eq!(tool_calls.len(), 1);
            }
            other => panic!("expected assistant, got {other:?}"),
        }
    }

    #[test]
    fn tool_output_maps_to_the_tool_role_keyed_by_call_id() {
        let msgs = items_to_messages(&[call("c1", "f"), call_output("c1", "42")])
            .expect("translate");
        match &msgs[1] {
            CompletionsMessage::Tool {
                tool_call_id,
                content,
            } => {
                assert_eq!(tool_call_id, "c1");
                assert_eq!(content, "42");
            }
            other => panic!("expected tool, got {other:?}"),
        }
    }

    #[test]
    fn an_orphan_tool_output_is_refused() {
        // Dropping it would mean the model never learns what its call returned, and
        // the provider would reject the unmatched id anyway. Failing here names the
        // real problem.
        let err = items_to_messages(&[user("q"), call_output("missing", "r")])
            .expect_err("an unmatched tool result must be refused");
        assert_eq!(
            err,
            TranslationError::OrphanToolOutput {
                call_id: "missing".into()
            }
        );
    }

    #[test]
    fn image_and_file_references_survive_translation() {
        let msgs = items_to_messages(&[ResponseItem::Message {
            role: Role::User,
            content: vec![
                ContentPart::InputText {
                    text: "what is this".into(),
                },
                ContentPart::InputImage {
                    image_url: Some("https://example.com/a.png".into()),
                    file_id: None,
                    detail: None,
                },
                ContentPart::InputFile {
                    file_id: Some("file_1".into()),
                    file_url: None,
                    filename: None,
                },
            ],
            id: None,
            status: None,
        }])
        .expect("translate");

        match &msgs[0] {
            CompletionsMessage::User { content } => {
                assert_eq!(content.len(), 3, "no part may be silently dropped");
                assert!(matches!(&content[1], CompletionsContent::ImageUrl { image_url } if image_url.url.contains("a.png")));
                assert!(matches!(&content[2], CompletionsContent::FileRef { id } if id == "file_1"));
            }
            other => panic!("expected user, got {other:?}"),
        }
    }

    #[test]
    fn developer_role_maps_to_system() {
        // Chat-completions has no developer role; mapping it to system preserves
        // the intent instead of dropping the message.
        let msgs = items_to_messages(&[ResponseItem::Message {
            role: Role::Developer,
            content: vec![ContentPart::InputText {
                text: "internal note".into(),
            }],
            id: None,
            status: None,
        }])
        .expect("translate");
        assert_eq!(msgs[0].role_name(), "system");
    }

}
