//! The outbound completions request type.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use nova_responses::protocol::{MetadataValue, Tool, ToolChoice, ToolChoiceMode};
use nova_responses::{RequestProvenance, ResponseItem};

use super::translate::{items_to_messages, TranslationError};

/// One completions request, ready to be scheduled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompletionsRequest {
    pub model: String,
    /// Conversation in provider order: system first, then oldest to newest.
    pub messages: Vec<CompletionsMessage>,
    /// Functions the model may call.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<CompletionsToolChoice>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_completion_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    /// Caller passthrough key-values (e.g. agent template selection), carried from
    /// the turn's params so an executor can dispatch on them. Never sent to the
    /// provider.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, MetadataValue>,
    /// Where this request came from. Never sent to the provider.
    pub provenance: RequestProvenance,
}

impl CompletionsRequest {
    /// Build from resolved context.
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
            tool_choice: None,
            max_completion_tokens: None,
            temperature: None,
            metadata: BTreeMap::new(),
            provenance,
        })
    }

    pub fn with_tools(mut self, tools: Vec<ToolSpec>) -> Self {
        self.tools = tools;
        self
    }

    pub fn with_tool_choice(mut self, tool_choice: Option<CompletionsToolChoice>) -> Self {
        self.tool_choice = tool_choice;
        self
    }

    pub fn with_metadata(mut self, metadata: BTreeMap<String, MetadataValue>) -> Self {
        self.metadata = metadata;
        self
    }

    /// The most recent user text, or `""`.
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

/// A message in provider shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum CompletionsMessage {
    System { content: String },
    User { content: Vec<CompletionsContent> },
    Assistant {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        refusal: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tool_calls: Vec<AssistantToolCall>,
    },
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CompletionsContent {
    Text { text: String },
    ImageUrl { image_url: ImageUrlPayload },
    FileRef { id: String },
}

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

/// A tool call on an assistant message, in chat-completions wire shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub function: AssistantFunction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantFunction {
    pub name: String,
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

/// A function offered to the model, in chat-completions wire shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolSpec {
    #[serde(rename = "type")]
    pub kind: String,
    pub function: FunctionSpec,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionSpec {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
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

/// The inbound protocol [`Tool`] is the caller's flat spelling; [`ToolSpec`] is
/// the outbound provider spelling (nested under `function`). This conversion is
/// the single place the two shapes meet, done here by the runner rather than the
/// service layer.
impl From<Tool> for ToolSpec {
    fn from(tool: Tool) -> Self {
        match tool {
            Tool::Function {
                name,
                description,
                parameters,
                strict,
            } => ToolSpec {
                kind: "function".to_string(),
                function: FunctionSpec {
                    name,
                    description,
                    parameters,
                    strict,
                },
            },
        }
    }
}

/// The function-name object nested inside a `tool_choice` specific selection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpecificFunction {
    pub name: String,
}

/// The outbound chat-completions shape of `tool_choice`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CompletionsToolChoice {
    Mode(ToolChoiceMode),
    Specific {
        #[serde(rename = "type")]
        kind: String,
        function: SpecificFunction,
    },
}

impl From<ToolChoice> for CompletionsToolChoice {
    fn from(choice: ToolChoice) -> Self {
        match choice {
            ToolChoice::Mode(mode) => CompletionsToolChoice::Mode(mode),
            ToolChoice::Function { name } => CompletionsToolChoice::Specific {
                kind: "function".to_string(),
                function: SpecificFunction { name },
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nova_responses::{Attempt, ContentPart, ResponseId, Role};

    fn provenance() -> RequestProvenance {
        RequestProvenance {
            response_id: ResponseId::parse("resp_node-a_00000000-0000-0000-0000-000000000001")
                .unwrap(),
            attempt: Attempt(1),
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
        for instructions in [None, Some("")] {
            let r = CompletionsRequest::from_context("m", instructions, &[user("hi")], provenance())
                .expect("build");
            assert_eq!(r.messages.len(), 1, "instructions={instructions:?}");
        }
    }

    #[test]
    fn empty_context_is_refused() {
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
    fn provenance_carries_the_fencing_token() {
        let r = CompletionsRequest::from_context(
            "m",
            None,
            &[user("hi")],
            RequestProvenance {
                response_id: ResponseId::parse("resp_node-b_00000000-0000-0000-0000-000000000009")
                    .unwrap(),
                attempt: Attempt(3),
                exec_deadline_ms: 12_345,
            },
        )
        .expect("build");
        assert_eq!(r.provenance.attempt, Attempt(3));
        assert_eq!(r.provenance.exec_deadline_ms, 12_345);
    }

    #[test]
    fn tool_calls_serialise_in_chat_completions_wire_shape() {
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
