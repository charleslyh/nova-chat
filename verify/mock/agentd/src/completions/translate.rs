//! `ResponseItem` → `CompletionsMessage`.

use thiserror::Error;

use super::request::{
    AssistantToolCall, CompletionsContent, CompletionsMessage, ImageUrlPayload,
};
use nova_responses::{ContentPart, ResponseItem, Role};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TranslationError {
    #[error("tool output for call `{call_id}` has no preceding call in this context")]
    OrphanToolOutput { call_id: String },
    #[error("context contains no messages to send")]
    EmptyContext,
}

/// Convert a resolved item sequence into provider messages.
pub fn items_to_messages(items: &[ResponseItem]) -> Result<Vec<CompletionsMessage>, TranslationError> {
    let mut out: Vec<CompletionsMessage> = Vec::with_capacity(items.len());
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
        ContentPart::Refusal { .. } => None,
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

    #[test]
    fn chronological_order_is_preserved() {
        let msgs = items_to_messages(&[user("q1"), user("q2")]).expect("translate");
        let roles: Vec<_> = msgs.iter().map(CompletionsMessage::role_name).collect();
        assert_eq!(roles, vec!["user", "user"]);
    }

    #[test]
    fn an_orphan_tool_output_is_refused() {
        let call_output = ResponseItem::FunctionCallOutput {
            call_id: "missing".into(),
            output: "r".into(),
            id: None,
            status: None,
        };
        let err = items_to_messages(&[user("q"), call_output]).expect_err("must refuse");
        assert_eq!(
            err,
            TranslationError::OrphanToolOutput {
                call_id: "missing".into()
            }
        );
    }
}
