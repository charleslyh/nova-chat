//! Response items — closed set (D22).
//!
//! Variants deliberately absent, and why:
//!
//! | absent type | reason |
//! |---|---|
//! | `item_reference` | lets a caller pull an arbitrary item by id, **bypassing the per-hop tenant check** of chain resolution (INV-52) |
//! | `reasoning` | encrypted reasoning payloads must be round-tripped verbatim; not in subset |
//! | `computer_call`, `program`, `mcp_*`, hosted tools | out of product scope |
//!
//! Deserialising any of them fails, producing a 400. That **is** the intended
//! behaviour, not a limitation to work around.

use serde::{Deserialize, Serialize};
use strum::AsRefStr;
use thiserror::Error;

use super::content::{ContentPart, ContentViolation};
use super::limits::ProtocolLimits;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    User,
    Assistant,
    System,
    Developer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemStatus {
    InProgress,
    Completed,
    Incomplete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, AsRefStr)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[strum(serialize_all = "snake_case")]
pub enum ResponseItem {
    Message {
        role: Role,
        content: Vec<ContentPart>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<ItemStatus>,
    },
    FunctionCall {
        call_id: String,
        name: String,
        /// JSON-encoded arguments, kept as an opaque string exactly as upstream
        /// does — re-parsing here would change what the model produced.
        arguments: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<ItemStatus>,
    },
    FunctionCallOutput {
        call_id: String,
        output: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<ItemStatus>,
    },
    /// A custom-tool call (upstream `custom_tool_call`): a free-form `input`
    /// string instead of JSON-schema'd `function_call` arguments. Borrowed here
    /// to carry tool-run-time, out-of-band render metadata (e.g. an
    /// authorization request) into the response stream — the model's arguments
    /// cannot express it, and it rides the standard `response.output_item.*`
    /// lifecycle like any other item.
    CustomToolCall {
        call_id: String,
        name: String,
        /// Free-form input (JSON-stringified render options), opaque passthrough.
        input: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<ItemStatus>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ItemViolation {
    #[error("message content must not be empty")]
    EmptyMessageContent,
    #[error("call_id must not be empty")]
    EmptyCallId,
    #[error("function name must not be empty")]
    EmptyFunctionName,
    #[error("item content invalid: {0}")]
    Content(#[from] ContentViolation),
}

impl ResponseItem {
    /// For metrics and diagnostics only — never branch business logic on this.
    pub fn item_type(&self) -> &str {
        self.as_ref()
    }

    /// The stream identity of an item: a tool `call_id` for tool items, the message
    /// id otherwise.
    ///
    /// Lives here rather than in the producer that streams it, because "which id
    /// does this item answer to" is a property of the item.
    pub fn stream_item_id(&self) -> &str {
        match self {
            ResponseItem::FunctionCall { call_id, .. }
            | ResponseItem::FunctionCallOutput { call_id, .. }
            | ResponseItem::CustomToolCall { call_id, .. } => call_id,
            ResponseItem::Message { id, .. } => id.as_deref().unwrap_or_default(),
        }
    }

    /// Chain closure predicate (INV-47 / CR-12).
    ///
    /// Every item this service can *emit* must be acceptable as *input* on the next
    /// turn, otherwise our own context chain breaks without any external caller
    /// being involved. Because the enum is closed and every variant is accepted on
    /// input, this is total — the assertion exists to catch a future variant being
    /// added on the output side only.
    pub fn is_acceptable_as_input(&self) -> bool {
        match self {
            ResponseItem::Message { .. }
            | ResponseItem::FunctionCall { .. }
            | ResponseItem::FunctionCallOutput { .. }
            | ResponseItem::CustomToolCall { .. } => true,
        }
    }

    /// Byte cost counted against chain and request budgets.
    pub fn byte_len(&self) -> usize {
        match self {
            ResponseItem::Message { content, .. } => {
                content.iter().map(ContentPart::byte_len).sum::<usize>()
            }
            ResponseItem::FunctionCall {
                call_id,
                name,
                arguments,
                ..
            } => call_id.len() + name.len() + arguments.len(),
            ResponseItem::FunctionCallOutput {
                call_id, output, ..
            } => call_id.len() + output.len(),
            ResponseItem::CustomToolCall {
                call_id, name, input, ..
            } => call_id.len() + name.len() + input.len(),
        }
    }

    pub fn validate(&self, limits: &ProtocolLimits) -> Result<(), ItemViolation> {
        match self {
            ResponseItem::Message { content, .. } => {
                if content.is_empty() {
                    return Err(ItemViolation::EmptyMessageContent);
                }
                for part in content {
                    part.validate_references(limits)?;
                }
                Ok(())
            }
            ResponseItem::FunctionCall { call_id, name, .. } => {
                if call_id.is_empty() {
                    return Err(ItemViolation::EmptyCallId);
                }
                if name.is_empty() {
                    return Err(ItemViolation::EmptyFunctionName);
                }
                Ok(())
            }
            ResponseItem::FunctionCallOutput { call_id, .. } => {
                if call_id.is_empty() {
                    return Err(ItemViolation::EmptyCallId);
                }
                Ok(())
            }
            ResponseItem::CustomToolCall { call_id, name, .. } => {
                if call_id.is_empty() {
                    return Err(ItemViolation::EmptyCallId);
                }
                if name.is_empty() {
                    return Err(ItemViolation::EmptyFunctionName);
                }
                Ok(())
            }
        }
    }

    /// Convenience constructor for a plain user text turn.
    pub fn user_text(text: impl Into<String>) -> Self {
        ResponseItem::Message {
            role: Role::User,
            content: vec![ContentPart::InputText { text: text.into() }],
            id: None,
            status: None,
        }
    }

    /// Convenience constructor for assistant output text.
    pub fn assistant_text(text: impl Into<String>) -> Self {
        ResponseItem::Message {
            role: Role::Assistant,
            content: vec![ContentPart::OutputText { text: text.into() }],
            id: None,
            status: Some(ItemStatus::Completed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> ProtocolLimits {
        ProtocolLimits::default()
    }

    #[test]
    fn round_trips_message() {
        let item = ResponseItem::user_text("hello");
        let json = serde_json::to_string(&item).unwrap();
        assert_eq!(
            json,
            r#"{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}"#
        );
        assert_eq!(serde_json::from_str::<ResponseItem>(&json).unwrap(), item);
    }

    #[test]
    fn round_trips_custom_tool_call() {
        let item = ResponseItem::CustomToolCall {
            call_id: "call_1".into(),
            name: "tool_authorization".into(),
            input: r#"{"resource":"db://prod","risk":"high"}"#.into(),
            id: Some("item_1".into()),
            status: Some(ItemStatus::Completed),
        };
        let json = serde_json::to_value(&item).unwrap();
        assert_eq!(json["type"], "custom_tool_call");
        assert_eq!(json["call_id"], "call_1");
        assert_eq!(json["name"], "tool_authorization");
        assert_eq!(json["input"], r#"{"resource":"db://prod","risk":"high"}"#);
        assert_eq!(serde_json::from_value::<ResponseItem>(json).unwrap(), item);
    }

    #[test]
    fn rejects_item_reference() {
        // The bypass vector: referencing an arbitrary item id would skip the
        // per-hop tenant check performed while walking the chain.
        let err = serde_json::from_str::<ResponseItem>(r#"{"type":"item_reference","id":"x"}"#)
            .expect_err("item_reference must be rejected");
        assert!(err.to_string().contains("item_reference"), "{err}");
    }

    #[test]
    fn rejects_reasoning_and_hosted_tool_items() {
        for json in [
            r#"{"type":"reasoning","summary":[],"encrypted_content":"zz"}"#,
            r#"{"type":"computer_call","call_id":"c","action":{}}"#,
            r#"{"type":"mcp_call","name":"n","arguments":"{}"}"#,
        ] {
            assert!(
                serde_json::from_str::<ResponseItem>(json).is_err(),
                "expected rejection for {json}"
            );
        }
    }

    #[test]
    fn rejects_unknown_field_on_known_item() {
        let err = serde_json::from_str::<ResponseItem>(
            r#"{"type":"function_call","call_id":"c","name":"n","arguments":"{}","extra":1}"#,
        )
        .expect_err("unknown field must be rejected");
        assert!(err.to_string().contains("extra"), "{err}");
    }

    #[test]
    fn every_variant_is_valid_input_chain_closure() {
        for item in [
            ResponseItem::user_text("a"),
            ResponseItem::assistant_text("b"),
            ResponseItem::FunctionCall {
                call_id: "c".into(),
                name: "n".into(),
                arguments: "{}".into(),
                id: None,
                status: None,
            },
            ResponseItem::FunctionCallOutput {
                call_id: "c".into(),
                output: "ok".into(),
                id: None,
                status: None,
            },
            ResponseItem::CustomToolCall {
                call_id: "c".into(),
                name: "tool_authorization".into(),
                input: r#"{"resource":"db://prod"}"#.into(),
                id: None,
                status: None,
            },
        ] {
            assert!(
                item.is_acceptable_as_input(),
                "chain closure violated by {}",
                item.item_type()
            );
            // And it must actually deserialise back as input.
            let json = serde_json::to_string(&item).unwrap();
            assert!(serde_json::from_str::<ResponseItem>(&json).is_ok());
        }
    }

    #[test]
    fn validate_rejects_empty_shapes() {
        let empty = ResponseItem::Message {
            role: Role::User,
            content: vec![],
            id: None,
            status: None,
        };
        assert_eq!(
            empty.validate(&limits()),
            Err(ItemViolation::EmptyMessageContent)
        );
        assert_eq!(
            ResponseItem::FunctionCall {
                call_id: String::new(),
                name: "n".into(),
                arguments: "{}".into(),
                id: None,
                status: None,
            }
            .validate(&limits()),
            Err(ItemViolation::EmptyCallId)
        );
    }

    #[test]
    fn stream_item_id_prefers_the_call_id() {
        assert_eq!(
            ResponseItem::FunctionCall {
                call_id: "call_1".into(),
                name: "n".into(),
                arguments: String::new(),
                id: Some("ignored".into()),
                status: None,
            }
            .stream_item_id(),
            "call_1"
        );
        assert_eq!(ResponseItem::user_text("x").stream_item_id(), "");
    }
}
