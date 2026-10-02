//! Protocol items → completion messages.
//!
//! Rescued from the retiring in-process runner. The stateful `ContextEngine`
//! wrapper around it was dropped: that existed to let a multi-round loop ingest
//! its own output between rounds, and this executor runs exactly one round, so
//! the translation is a pure function of the task.
//!
//! # Known limits, stated rather than discovered
//!
//! - **Text only.** `input_image` / `input_file` parts have no representation
//!   here and are rejected rather than silently dropped. Media reaching a model
//!   is a protocol-level capability that does not exist yet; failing loudly is
//!   what keeps that honest.
//! - **A tool output must follow its call.** A `function_call_output` whose
//!   `call_id` was not introduced earlier in the same item list fails the turn.
//!   Providers reject such a message anyway, and failing here names the cause.
//! - **`custom_tool_call` is not context.** It carries tool run-time render
//!   metadata (an authorization prompt, a sub-agent announcement) and is skipped.

use std::collections::{BTreeMap, HashSet};

use nova_agent_runtime::AgentTask;
use nova_responses::{ContentPart, ResponseItem, Role};

use crate::wire::{ModelError, RequestMessage, ToolCallRequest};

/// Build this turn's messages: the caller's instructions, then the conversation.
///
/// There is deliberately no second prompt source. Prompt composition —
/// preambles, agent personas, skill catalogues — belongs to whoever drives the
/// chain and arrives here already assembled in `instructions`, which is a
/// per-request protocol field. A `base_preamble` parameter here would be an
/// open invitation for agent configuration to drift back into this tier.
pub fn messages_for_task(task: &AgentTask) -> Result<Vec<RequestMessage>, ModelError> {
    let mut messages = Vec::new();
    push_system(&mut messages, task.params.instructions.as_deref());
    append_items(&mut messages, &task.items, 0, &task.reasoning)?;
    keep_reasoning_of_current_loop(&mut messages);
    if messages.is_empty() {
        return Err(ModelError::new("response context contains no messages"));
    }
    Ok(messages)
}

pub(crate) fn with_instructions(
    instructions: Option<&str>,
    mut conversation: Vec<RequestMessage>,
) -> Vec<RequestMessage> {
    let mut messages = Vec::new();
    push_system(&mut messages, instructions);
    messages.append(&mut conversation);
    messages
}

/// Drop the reasoning of every assistant turn before the last user message.
///
/// Interleaved-thinking models need their reasoning back within the current
/// tool loop, which starts at the user's latest message; earlier loops are
/// settled, and replaying their thinking would only grow the context.
pub(crate) fn keep_reasoning_of_current_loop(messages: &mut [RequestMessage]) {
    let Some(last_user) = messages
        .iter()
        .rposition(|message| matches!(message, RequestMessage::User { .. }))
    else {
        return;
    };
    for message in &mut messages[..last_user] {
        if let RequestMessage::Assistant { reasoning, .. } = message {
            *reasoning = None;
        }
    }
}

/// Append protocol items onto an existing completion transcript.
///
/// `first_index` is the position of `items[0]` in the task's full item list,
/// which is what `reasoning` is keyed by.
///
/// Tool outputs must still follow their call. Calls already present on
/// `messages` count, so a later turn can attach an output to a call that
/// survived in a cached prefix.
pub(crate) fn append_items(
    messages: &mut Vec<RequestMessage>,
    items: &[ResponseItem],
    first_index: usize,
    reasoning: &BTreeMap<usize, String>,
) -> Result<(), ModelError> {
    let mut known_calls = known_call_ids(messages);

    for (offset, item) in items.iter().enumerate() {
        let thought = reasoning.get(&(first_index + offset)).cloned();
        match item {
            ResponseItem::Message { role, content, .. } => {
                let text = flatten_text(content)?;
                messages.push(match role {
                    Role::User => RequestMessage::User { content: text },
                    Role::Assistant => RequestMessage::Assistant {
                        content: text,
                        tool_calls: None,
                        reasoning: thought,
                    },
                    // Developer collapses into System: the wire subset this
                    // transport targets has no separate developer role.
                    Role::System | Role::Developer => RequestMessage::System { content: text },
                });
            }
            ResponseItem::FunctionCall {
                call_id,
                name,
                arguments,
                ..
            } => {
                known_calls.insert(call_id.clone());
                let call = ToolCallRequest {
                    call_id: call_id.clone(),
                    name: name.clone(),
                    // Arguments are opaque model output; if they are not valid
                    // JSON, carry the raw text rather than failing the turn.
                    arguments: serde_json::from_str(arguments)
                        .unwrap_or_else(|_| serde_json::Value::String(arguments.clone())),
                };
                // Calls join the assistant turn right before them: either the
                // parallel calls of one round, or the text the same round said
                // first. A new round always starts after a tool output or a
                // user message, so an adjacent assistant turn is this round's.
                match messages.last_mut() {
                    Some(RequestMessage::Assistant {
                        tool_calls,
                        reasoning,
                        ..
                    }) => {
                        tool_calls.get_or_insert_with(Vec::new).push(call);
                        if reasoning.is_none() {
                            *reasoning = thought;
                        }
                    }
                    _ => messages.push(RequestMessage::Assistant {
                        content: String::new(),
                        tool_calls: Some(vec![call]),
                        reasoning: thought,
                    }),
                }
            }
            ResponseItem::FunctionCallOutput {
                call_id, output, ..
            } => {
                if !known_calls.contains(call_id) {
                    return Err(ModelError::new(format!(
                        "tool output for call `{call_id}` has no preceding function call"
                    )));
                }
                messages.push(RequestMessage::Tool {
                    content: output.clone(),
                    call_id: call_id.clone(),
                });
            }
            ResponseItem::CustomToolCall { .. } => {}
        }
    }

    Ok(())
}

fn push_system(messages: &mut Vec<RequestMessage>, value: Option<&str>) {
    if let Some(content) = value.map(str::trim).filter(|value| !value.is_empty()) {
        messages.push(RequestMessage::System {
            content: content.to_string(),
        });
    }
}

fn known_call_ids(messages: &[RequestMessage]) -> HashSet<String> {
    let mut known = HashSet::new();
    for message in messages {
        if let RequestMessage::Assistant {
            tool_calls: Some(calls),
            ..
        } = message
        {
            known.extend(calls.iter().map(|call| call.call_id.clone()));
        }
    }
    known
}

#[cfg(test)]
fn items_to_messages(items: &[ResponseItem]) -> Result<Vec<RequestMessage>, ModelError> {
    let mut messages = Vec::with_capacity(items.len());
    append_items(&mut messages, items, 0, &BTreeMap::new())?;
    Ok(messages)
}

fn flatten_text(parts: &[ContentPart]) -> Result<String, ModelError> {
    let mut text = String::new();
    for part in parts {
        match part {
            ContentPart::InputText { text: value } | ContentPart::OutputText { text: value } => {
                text.push_str(value)
            }
            // A refusal is the assistant's own words for this turn; keeping it
            // as text is what lets a later turn see why it declined.
            ContentPart::Refusal { refusal } => text.push_str(refusal),
            ContentPart::InputImage { .. } | ContentPart::InputFile { .. } => {
                return Err(ModelError::new(format!(
                    "content type `{}` cannot be converted to a text-only context",
                    part.part_type()
                )));
            }
        }
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(text: &str) -> ResponseItem {
        ResponseItem::user_text(text)
    }

    fn call(call_id: &str, name: &str, arguments: &str) -> ResponseItem {
        ResponseItem::FunctionCall {
            call_id: call_id.to_string(),
            name: name.to_string(),
            arguments: arguments.to_string(),
            id: None,
            status: None,
        }
    }

    fn output(call_id: &str, output: &str) -> ResponseItem {
        ResponseItem::FunctionCallOutput {
            call_id: call_id.to_string(),
            output: output.to_string(),
            id: None,
            status: None,
        }
    }

    #[test]
    fn a_plain_question_becomes_one_user_message() {
        let messages = items_to_messages(&[user("hello")]).expect("translates");
        assert_eq!(
            messages,
            vec![RequestMessage::User {
                content: "hello".to_string()
            }]
        );
    }

    #[test]
    fn parallel_calls_share_one_assistant_message() {
        let messages = items_to_messages(&[
            user("draw two things"),
            call("c1", "draw", r#"{"a":1}"#),
            call("c2", "draw", r#"{"a":2}"#),
        ])
        .expect("translates");

        assert_eq!(messages.len(), 2, "one user turn, one assistant turn");
        match &messages[1] {
            RequestMessage::Assistant { tool_calls, .. } => {
                let calls = tool_calls.as_ref().expect("calls present");
                assert_eq!(calls.len(), 2, "both calls ride the same message");
                assert_eq!(calls[0].call_id, "c1");
                assert_eq!(calls[1].call_id, "c2");
            }
            other => panic!("expected an assistant message, got {other:?}"),
        }
    }

    #[test]
    fn a_call_followed_by_its_output_round_trips() {
        let messages = items_to_messages(&[user("q"), call("c1", "t", "{}"), output("c1", "done")])
            .expect("translates");
        assert_eq!(messages.len(), 3);
        assert_eq!(
            messages[2],
            RequestMessage::Tool {
                content: "done".to_string(),
                call_id: "c1".to_string()
            }
        );
    }

    #[test]
    fn a_tool_output_without_its_call_fails_the_turn() {
        let error = items_to_messages(&[user("q"), output("ghost", "done")])
            .expect_err("must not be silently dropped");
        assert!(
            error.to_string().contains("ghost"),
            "the error names the offending call id: {error}"
        );
    }

    #[test]
    fn custom_tool_calls_never_reach_the_model() {
        let messages = items_to_messages(&[
            user("q"),
            ResponseItem::CustomToolCall {
                call_id: "c1".to_string(),
                name: "tool_authorization".to_string(),
                input: "{}".to_string(),
                id: None,
                status: None,
            },
        ])
        .expect("translates");
        assert_eq!(messages.len(), 1, "only the user turn is context");
    }

    #[test]
    fn developer_role_collapses_into_system() {
        let messages = items_to_messages(&[ResponseItem::Message {
            role: Role::Developer,
            content: vec![ContentPart::InputText {
                text: "be brief".to_string(),
            }],
            id: None,
            status: None,
        }])
        .expect("translates");
        assert_eq!(
            messages,
            vec![RequestMessage::System {
                content: "be brief".to_string()
            }]
        );
    }

    #[test]
    fn media_parts_are_rejected_rather_than_dropped() {
        let error = flatten_text(&[ContentPart::InputImage {
            image_url: Some("https://example.com/a.png".to_string()),
            file_id: None,
            detail: None,
        }])
        .expect_err("text-only context must refuse media");
        assert!(
            error.to_string().contains("input_image"),
            "the error names the part type: {error}"
        );
    }

    #[test]
    fn a_refusal_is_carried_as_text() {
        let text = flatten_text(&[ContentPart::Refusal {
            refusal: "I cannot".to_string(),
        }])
        .expect("refusals are text");
        assert_eq!(text, "I cannot");
    }

    #[test]
    fn malformed_call_arguments_are_carried_verbatim() {
        let messages = items_to_messages(&[call("c1", "t", "{oops")]).expect("translates");
        match &messages[0] {
            RequestMessage::Assistant { tool_calls, .. } => {
                let calls = tool_calls.as_ref().expect("calls present");
                assert_eq!(
                    calls[0].arguments,
                    serde_json::Value::String("{oops".to_string())
                );
            }
            other => panic!("expected an assistant message, got {other:?}"),
        }
    }

    fn assistant(text: &str) -> ResponseItem {
        ResponseItem::assistant_text(text)
    }

    fn with_reasoning(items: &[ResponseItem], reasoning: &[(usize, &str)]) -> Vec<RequestMessage> {
        let map = reasoning
            .iter()
            .map(|(index, text)| (*index, text.to_string()))
            .collect();
        let mut messages = Vec::new();
        append_items(&mut messages, items, 0, &map).expect("translates");
        keep_reasoning_of_current_loop(&mut messages);
        messages
    }

    fn reasoning_of(message: &RequestMessage) -> Option<&str> {
        match message {
            RequestMessage::Assistant { reasoning, .. } => reasoning.as_deref(),
            _ => None,
        }
    }

    /// The interleaved-thinking contract: within the current tool loop every
    /// assistant turn goes back with the reasoning it was produced with.
    #[test]
    fn reasoning_rides_the_assistant_turn_it_preceded() {
        let messages = with_reasoning(
            &[
                user("q"),
                call("c1", "t", "{}"),
                output("c1", "r1"),
                assistant("done"),
            ],
            &[(1, "plan"), (3, "conclude")],
        );
        assert_eq!(reasoning_of(&messages[1]), Some("plan"));
        assert_eq!(reasoning_of(&messages[3]), Some("conclude"));
    }

    #[test]
    fn reasoning_before_the_latest_user_message_is_dropped() {
        let messages = with_reasoning(
            &[
                user("q1"),
                assistant("a1"),
                user("q2"),
                call("c1", "t", "{}"),
            ],
            &[(1, "old"), (3, "new")],
        );
        assert_eq!(reasoning_of(&messages[1]), None, "a settled loop");
        assert_eq!(reasoning_of(&messages[3]), Some("new"));
    }

    /// One round that says what it will do and then calls is one assistant
    /// turn, the way the model produced it, carrying that round's reasoning.
    #[test]
    fn a_rounds_text_and_its_calls_share_one_assistant_turn() {
        let messages = with_reasoning(
            &[user("q"), assistant("on it"), call("c1", "t", "{}")],
            &[(1, "plan")],
        );
        assert_eq!(messages.len(), 2);
        match &messages[1] {
            RequestMessage::Assistant {
                content,
                tool_calls,
                reasoning,
            } => {
                assert_eq!(content, "on it");
                assert_eq!(tool_calls.as_ref().map(Vec::len), Some(1));
                assert_eq!(reasoning.as_deref(), Some("plan"));
            }
            other => panic!("expected an assistant message, got {other:?}"),
        }
    }

    #[test]
    fn reasoning_is_keyed_by_the_position_in_the_full_item_list() {
        let map = BTreeMap::from([(5, "late".to_string())]);
        let mut messages = Vec::new();
        append_items(&mut messages, &[assistant("a")], 5, &map).expect("translates");
        assert_eq!(reasoning_of(&messages[0]), Some("late"));
    }
}
