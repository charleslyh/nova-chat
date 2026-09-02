//! The outbound request type.

use serde::{Deserialize, Serialize};

use super::translate::{items_to_messages, TranslationError};
use crate::protocol::ResponseItem;

/// One completions request, ready to be scheduled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompletionsRequest {
    pub model: String,

    /// Conversation in provider order: system first, then oldest to newest.
    pub messages: Vec<CompletionsMessage>,

    /// Functions the model may call. Empty means none are offered — which is not
    /// the same as the model being forbidden to emit one, so a scheduler must
    /// still tolerate an unexpected call rather than panicking.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolSpec>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_completion_tokens: Option<u32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,

    /// Where this request came from. Never sent to the provider; carried so a
    /// scheduler can log, attribute cost, and honour the execution deadline.
    pub provenance: RequestProvenance,
}

impl CompletionsRequest {
    /// Build from resolved context.
    ///
    /// `instructions` becomes the leading system message. It is never inherited
    /// across turns (FR-19), so its absence means the caller sent none *this*
    /// turn — not that it was lost.
    pub fn from_context(
        model: impl Into<String>,
        instructions: Option<&str>,
        items: &[ResponseItem],
        provenance: RequestProvenance,
    ) -> Result<Self, TranslationError> {
        let mut messages = Vec::with_capacity(items.len() + 1);

        if let Some(text) = instructions {
            if !text.is_empty() {
                messages.push(CompletionsMessage::System {
                    content: text.to_string(),
                });
            }
        }

        messages.extend(items_to_messages(items)?);

        if messages.is_empty() {
            return Err(TranslationError::EmptyContext);
        }

        Ok(Self {
            model: model.into(),
            messages,
            tools: Vec::new(),
            max_completion_tokens: None,
            temperature: None,
            provenance,
        })
    }

    pub fn with_tools(mut self, tools: Vec<ToolSpec>) -> Self {
        self.tools = tools;
        self
    }

    /// The most recent user text, or `""`.
    ///
    /// Provided because every test scheduler needs it, and hand-rolling the walk
    /// each time invites subtly different answers to "which message is last".
    pub fn last_user_text(&self) -> &str {
        for m in self.messages.iter().rev() {
            if let CompletionsMessage::User { content } = m {
                for part in content.iter().rev() {
                    if let CompletionsContent::Text { text } = part {
                        return text;
                    }
                }
            }
        }
        ""
    }

    /// Total characters across all message text, for rough budgeting.
    pub fn approx_input_chars(&self) -> usize {
        self.messages
            .iter()
            .map(CompletionsMessage::approx_chars)
            .sum()
    }
}

/// Traceability for one request. Not part of the provider payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestProvenance {
    pub response_id: String,
    /// Fencing token. A scheduler that ignores this cannot tell its work was
    /// superseded, and will keep spending tokens on an abandoned attempt.
    pub attempt: u64,
    /// Wall-clock milliseconds after which this attempt is forfeit.
    pub exec_deadline_ms: u64,
}

/// A message in provider shape.
///
/// Note this is *not* [`ResponseItem`]. The differences are deliberate and are
/// what [`super::translate`] exists to reconcile: completions has a distinct
/// `tool` role, and it groups parallel tool calls onto a single assistant message
/// rather than emitting one item each.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum CompletionsMessage {
    /// Carries `instructions`.
    System { content: String },
    User { content: Vec<CompletionsContent> },
    Assistant {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content: Option<String>,
        /// A refusal, carried at message level — chat completions has no refusal
        /// content part; it is the assistant message's `refusal` field.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        refusal: Option<String>,
        /// Parallel calls belong to one assistant message.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tool_calls: Vec<AssistantToolCall>,
    },
    /// Result of a call, matched back by `tool_call_id`.
    Tool {
        tool_call_id: String,
        content: String,
    },
}

impl CompletionsMessage {
    pub fn user_text(text: impl Into<String>) -> Self {
        CompletionsMessage::User {
            content: vec![CompletionsContent::Text { text: text.into() }],
        }
    }

    pub fn assistant_text(text: impl Into<String>) -> Self {
        CompletionsMessage::Assistant {
            content: Some(text.into()),
            refusal: None,
            tool_calls: Vec::new(),
        }
    }

    pub fn role_name(&self) -> &'static str {
        match self {
            CompletionsMessage::System { .. } => "system",
            CompletionsMessage::User { .. } => "user",
            CompletionsMessage::Assistant { .. } => "assistant",
            CompletionsMessage::Tool { .. } => "tool",
        }
    }

    fn approx_chars(&self) -> usize {
        match self {
            CompletionsMessage::System { content }
            | CompletionsMessage::Tool { content, .. } => content.len(),
            CompletionsMessage::User { content } => {
                content.iter().map(CompletionsContent::approx_chars).sum()
            }
            CompletionsMessage::Assistant {
                content,
                tool_calls,
                ..
            } => {
                content.as_ref().map_or(0, String::len)
                    + tool_calls
                        .iter()
                        .map(|c| c.function.name.len() + c.function.arguments.len())
                        .sum::<usize>()
            }
        }
    }
}

/// A content part in provider shape.
///
/// Images and files are **references only** — there is no inline-bytes variant,
/// mirroring the inbound subset (SEC-6). A scheduler therefore cannot be handed a
/// payload whose size was never budgeted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CompletionsContent {
    Text { text: String },
    /// `{ "type": "image_url", "image_url": { "url": "..." } }` — chat
    /// completions nests the URL inside an `image_url` object.
    ImageUrl { image_url: ImageUrlPayload },
    /// A file reference. Not a chat-completions content part; it is this
    /// service's extension for carrying `input_file` items to a provider that
    /// supports them. A strict chat-completions adapter must translate it.
    FileRef { id: String },
}

/// The `image_url` object nested inside an image content part.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageUrlPayload {
    pub url: String,
}

impl CompletionsContent {
    fn approx_chars(&self) -> usize {
        match self {
            CompletionsContent::Text { text } => text.len(),
            CompletionsContent::ImageUrl { image_url } => image_url.url.len(),
            CompletionsContent::FileRef { id } => id.len(),
        }
    }
}

/// A tool call on an assistant message, in chat-completions wire shape:
/// `{ "id", "type": "function", "function": { "name", "arguments" } }`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantToolCall {
    pub id: String,
    /// Always `"function"` — the only tool-call kind chat completions carries.
    #[serde(rename = "type")]
    pub kind: String,
    pub function: AssistantFunction,
}

/// The `function` object nested inside a tool call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantFunction {
    pub name: String,
    /// JSON kept as an opaque string, exactly as the provider produced it.
    /// Re-encoding would change what the model said.
    pub arguments: String,
}

impl AssistantToolCall {
    pub fn new(id: String, name: String, arguments: String) -> Self {
        Self {
            id,
            kind: "function".to_string(),
            function: AssistantFunction { name, arguments },
        }
    }
}

/// A function offered to the model, in chat-completions wire shape:
/// `{ "type": "function", "function": { "name", "description", "parameters", "strict" } }`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolSpec {
    /// Always `"function"` — the only tool kind this service offers.
    #[serde(rename = "type")]
    pub kind: String,
    pub function: FunctionSpec,
}

/// The `function` object nested inside a tool definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionSpec {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// JSON Schema, opaque here.
    pub parameters: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strict: Option<bool>,
}

impl ToolSpec {
    pub fn new(name: String, description: Option<String>, parameters: serde_json::Value) -> Self {
        Self {
            kind: "function".to_string(),
            function: FunctionSpec {
                name,
                description,
                parameters,
                strict: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ContentPart, Role};

    fn provenance() -> RequestProvenance {
        RequestProvenance {
            response_id: "resp_node-a_1".into(),
            attempt: 1,
            exec_deadline_ms: 60_000,
        }
    }

    fn user(text: &str) -> ResponseItem {
        ResponseItem::Message {
            role: Role::User,
            content: vec![ContentPart::InputText { text: text.into() }],
            id: None,
            status: None,
        }
    }

    #[test]
    fn instructions_lead_as_a_system_message() {
        let r =
            CompletionsRequest::from_context("m", Some("be brief"), &[user("hi")], provenance())
                .expect("build");
        assert_eq!(r.messages.len(), 2);
        assert_eq!(r.messages[0].role_name(), "system");
        assert_eq!(r.messages[1].role_name(), "user");
    }

    #[test]
    fn absent_or_empty_instructions_add_no_message() {
        // Instructions do not cross turns (FR-19). An empty system message would
        // be a different prompt from no system message at all.
        for instructions in [None, Some("")] {
            let r = CompletionsRequest::from_context("m", instructions, &[user("hi")], provenance())
                .expect("build");
            assert_eq!(r.messages.len(), 1, "instructions={instructions:?}");
        }
    }

    #[test]
    fn empty_context_is_refused() {
        // Sending a request with no messages would spend a call to be told the
        // input was invalid.
        assert_eq!(
            CompletionsRequest::from_context("m", None, &[], provenance())
                .expect_err("must refuse"),
            TranslationError::EmptyContext
        );
    }

    #[test]
    fn last_user_text_picks_the_most_recent() {
        let r = CompletionsRequest::from_context(
            "m",
            None,
            &[
                user("old"),
                ResponseItem::Message {
                    role: Role::Assistant,
                    content: vec![ContentPart::OutputText {
                        text: "reply".into(),
                    }],
                    id: None,
                    status: None,
                },
                user("new"),
            ],
            provenance(),
        )
        .expect("build");
        assert_eq!(r.last_user_text(), "new");
    }

    #[test]
    fn last_user_text_is_empty_when_absent() {
        // A tool-result-only turn is legal, and every scheduler would otherwise
        // need the same guard.
        let r = CompletionsRequest {
            model: "m".into(),
            messages: vec![CompletionsMessage::assistant_text("only me")],
            tools: vec![],
            max_completion_tokens: None,
            temperature: None,
            provenance: provenance(),
        };
        assert_eq!(r.last_user_text(), "");
    }

    #[test]
    fn a_request_round_trips_through_json() {
        // Required for a failing integration test to be reproducible from its log:
        // the request is the whole input.
        let r = CompletionsRequest {
            model: "m".into(),
            messages: vec![
                CompletionsMessage::System {
                    content: "be brief".into(),
                },
                CompletionsMessage::user_text("hi"),
                CompletionsMessage::Assistant {
                    content: None,
                    refusal: None,
                    tool_calls: vec![AssistantToolCall::new(
                        "call_1".into(),
                        "lookup".into(),
                        r#"{"q":"x"}"#.into(),
                    )],
                },
                CompletionsMessage::Tool {
                    tool_call_id: "call_1".into(),
                    content: "found".into(),
                },
            ],
            tools: vec![],
            max_completion_tokens: None,
            temperature: None,
            provenance: provenance(),
        };
        let json = serde_json::to_string(&r).expect("serialise");
        assert_eq!(
            serde_json::from_str::<CompletionsRequest>(&json).expect("deserialise"),
            r
        );
    }

    #[test]
    fn tool_arguments_are_not_reencoded() {
        // Whitespace inside the arguments string is part of what the model
        // produced; re-emitting would alter it and any signature over the
        // transcript would then disagree.
        let raw = r#"{ "a" :  1 }"#;
        let m = CompletionsMessage::Assistant {
            content: None,
            refusal: None,
            tool_calls: vec![AssistantToolCall::new("c".into(), "f".into(), raw.into())],
        };
        let back: CompletionsMessage =
            serde_json::from_str(&serde_json::to_string(&m).expect("ser")).expect("de");
        match back {
            CompletionsMessage::Assistant { tool_calls, .. } => {
                assert_eq!(tool_calls[0].function.arguments, raw)
            }
            other => panic!("expected assistant, got {other:?}"),
        }
    }

    #[test]
    fn provenance_carries_the_fencing_token() {
        let r = CompletionsRequest::from_context(
            "m",
            None,
            &[user("hi")],
            RequestProvenance {
                response_id: "resp_node-b_9".into(),
                attempt: 3,
                exec_deadline_ms: 12_345,
            },
        )
        .expect("build");
        assert_eq!(r.provenance.attempt, 3);
        assert_eq!(r.provenance.exec_deadline_ms, 12_345);
    }

    #[test]
    fn tool_calls_serialise_in_chat_completions_wire_shape() {
        // The provider contract: `{ id, type: "function", function: { name, arguments } }`.
        // A flat `{ id, name, arguments }` would be rejected by chat completions.
        let call = AssistantToolCall::new("call_1".into(), "lookup".into(), r#"{"q":"x"}"#.into());
        let json = serde_json::to_value(&call).expect("serialise");
        assert_eq!(json["type"], "function");
        assert_eq!(json["function"]["name"], "lookup");
        assert_eq!(json["function"]["arguments"], r#"{"q":"x"}"#);
        assert!(json.get("name").is_none(), "no flat `name` field: {json}");
    }

    #[test]
    fn tool_specs_serialise_in_chat_completions_wire_shape() {
        let spec = ToolSpec::new(
            "lookup".into(),
            Some("look up a thing".into()),
            serde_json::json!({ "type": "object" }),
        );
        let json = serde_json::to_value(&spec).expect("serialise");
        assert_eq!(json["type"], "function");
        assert_eq!(json["function"]["name"], "lookup");
        assert_eq!(json["function"]["parameters"]["type"], "object");
    }
}
