//! The OpenAI-shaped `response` object (D22).
//!
//! Rendering lives in the protocol module, not on the domain record: the record is
//! pure data, and the wire constants (`"object": "response"`, `created_at` in
//! seconds) belong to the protocol. It is a **struct**, not a `json!` literal —
//! the literal silently omitted `tools` and `tool_choice` for months, and nothing
//! could have caught that, whereas a struct field is a compile-time obligation.
//!
//! It is also the payload of every lifecycle event, which is what lets a
//! subscriber that joins mid-turn receive the whole response, and what lets a
//! bare chain be reconstructed from its stream — from a **typed** `output`, not by
//! reaching into a JSON map with a string key.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::response::{ResponseRecord, ResponseStatus};
use crate::usage::Usage;

use super::item::ResponseItem;
use super::metadata::MetadataValue;
use super::request::ConversationRef;
use super::tool::{Tool, ToolChoice};

/// The `object` discriminator. A one-variant enum rather than a `String`: the
/// value is a protocol constant, so it should not be possible to render an object
/// claiming to be something else.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ObjectKind {
    #[default]
    #[serde(rename = "response")]
    Response,
}

/// The response object returned by `GET /v1/responses/{id}` and embedded in
/// lifecycle events.
///
/// Only protocol fields appear here — internal bookkeeping (`tenant_id`,
/// `attempt`, `owner`, `integrity`) never leaves the node, which is enforced by
/// this type having nowhere to put it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponseObject {
    pub id: String,
    #[serde(default)]
    pub object: ObjectKind,
    /// Unix **seconds**, as upstream reports it.
    pub created_at: u64,
    pub status: ResponseStatus,
    pub model: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_response_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation: Option<ConversationRef>,
    /// Always rendered, even when `None`: `instructions` is a fixed `string | null`
    /// protocol field. Omitting it would read as "echo the inherited instructions"
    /// rather than "this turn has none" (INV-49).
    #[serde(default)]
    pub instructions: Option<String>,

    pub store: bool,

    #[serde(default)]
    pub input: Vec<ResponseItem>,
    #[serde(default)]
    pub output: Vec<ResponseItem>,

    /// Render-only reasoning text, never fed back as context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<Tool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoice>,

    /// Echoed from the caller's request metadata (see `ModelParams::metadata`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, MetadataValue>,

    #[serde(default)]
    pub usage: Usage,
}

impl ResponseObject {
    /// Render a record whose output is known.
    ///
    /// `output` is supplied by the caller rather than read from the record: the
    /// record holds no output (it lives in the conversation snapshot and the event
    /// stream), so the caller passes in what it committed or reconstructed.
    pub fn new(record: &ResponseRecord, output: Vec<ResponseItem>) -> Self {
        let spec = &record.spec;
        let params = &spec.params;
        Self {
            id: record.response_id.to_string(),
            object: ObjectKind::Response,
            created_at: record.created_at_ms / 1000,
            status: record.status,
            model: params.model.clone(),
            previous_response_id: record.previous_response_id().map(|id| id.to_string()),
            conversation: record.conversation_id().map(|id| ConversationRef::Object {
                id: id.to_string(),
            }),
            instructions: params.instructions.clone(),
            store: spec.store,
            input: spec.input_items.clone(),
            output,
            reasoning: record.reasoning.clone(),
            tools: params.tools.clone(),
            tool_choice: params.tool_choice.clone(),
            metadata: params.metadata.clone(),
            usage: record.usage,
        }
    }

    /// Render a record with no output yet (create, in-progress, failure).
    pub fn without_output(record: &ResponseRecord) -> Self {
        Self::new(record, Vec::new())
    }

    /// The minimum an event needs to end a stream when the record is not in hand.
    ///
    /// Used on the reap path, where reading the record back would be an extra
    /// query per abandoned claim purely to enrich an event whose only job is to
    /// terminate the stream. Subscribers that want the finished object use `GET`.
    pub fn terminal_stub(id: &crate::response::ResponseId, status: ResponseStatus) -> Self {
        Self {
            id: id.to_string(),
            object: ObjectKind::Response,
            created_at: 0,
            status,
            model: String::new(),
            previous_response_id: None,
            conversation: None,
            instructions: None,
            store: false,
            input: Vec::new(),
            output: Vec::new(),
            reasoning: None,
            tools: Vec::new(),
            tool_choice: None,
            metadata: BTreeMap::new(),
            usage: Usage::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{IdempotencyKey, NodeTag, TenantId};
    use crate::protocol::ToolChoiceMode;
    use crate::response::{ContextAnchor, ModelParams, ResponseId, TurnSpec};
    use crate::ConversationId;

    fn record() -> ResponseRecord {
        ResponseRecord::queued(
            ResponseId::new(NodeTag::parse("n1").unwrap()),
            TenantId::parse("t1").unwrap(),
            TurnSpec {
                params: ModelParams {
                    model: "m".into(),
                    instructions: Some("be brief".into()),
                    tools: vec![Tool::Function {
                        name: "f".into(),
                        description: None,
                        parameters: serde_json::json!({}),
                        strict: None,
                    }],
                    tool_choice: Some(ToolChoice::Mode(ToolChoiceMode::Auto)),
                    metadata: BTreeMap::new(),
                },
                input_items: vec![ResponseItem::user_text("hi")],
                store: true,
                ext: None,
                anchor: ContextAnchor::Conversation(ConversationId::new()),
            },
            IdempotencyKey::parse("k").unwrap(),
            12_345,
            1_000,
        )
    }

    #[test]
    fn renders_every_field_the_record_carries() {
        // The regression this type exists for: the previous `json!` renderer
        // dropped `tools` and `tool_choice` because a macro cannot notice a
        // missing key.
        let json = serde_json::to_value(ResponseObject::without_output(&record())).unwrap();
        assert_eq!(json["object"], "response");
        assert_eq!(json["created_at"], 12);
        assert_eq!(json["status"], "queued");
        assert_eq!(json["model"], "m");
        assert_eq!(json["store"], true);
        assert_eq!(json["instructions"], "be brief");
        assert_eq!(json["tools"][0]["name"], "f");
        assert_eq!(json["tool_choice"], "auto");
        assert!(json["conversation"]["id"].as_str().unwrap().starts_with("conv_"));
        assert_eq!(json["usage"]["total_tokens"], 0);
    }

    #[test]
    fn internal_bookkeeping_never_reaches_the_wire() {
        let json = serde_json::to_value(ResponseObject::without_output(&record())).unwrap();
        for leaked in [
            "tenant_id",
            "attempt",
            "owner",
            "integrity",
            "idempotency_key",
            "expires_at_ms",
        ] {
            assert!(json.get(leaked).is_none(), "{leaked} leaked: {json}");
        }
    }

    #[test]
    fn absent_optionals_are_omitted_but_instructions_is_null() {
        let mut r = record();
        r.spec.params.instructions = None;
        r.spec.params.tools.clear();
        r.spec.params.tool_choice = None;
        r.spec.anchor = ContextAnchor::Root;
        let json = serde_json::to_value(ResponseObject::without_output(&r)).unwrap();
        for absent in [
            "tools",
            "tool_choice",
            "conversation",
            "previous_response_id",
            "reasoning",
        ] {
            assert!(json.get(absent).is_none(), "{absent} should be omitted: {json}");
        }
        // `instructions` is a fixed protocol field: `None` renders as `null` rather
        // than disappearing (INV-49 — a second turn's instructions do not carry over).
        assert_eq!(json.get("instructions"), Some(&serde_json::Value::Null));
    }

    #[test]
    fn caller_metadata_round_trips_and_empty_metadata_is_omitted() {
        // Passthrough key-values reach the rendered object verbatim; an empty map
        // is omitted so the common case stays byte-identical to before the field
        // existed.
        let mut r = record();
        r.spec.params.metadata.insert(
            "agent_id".into(),
            MetadataValue::String("decoupage".into()),
        );
        let json = serde_json::to_value(ResponseObject::without_output(&r)).unwrap();
        assert_eq!(json["metadata"]["agent_id"], "decoupage");

        let bare = serde_json::to_value(ResponseObject::without_output(&record())).unwrap();
        assert!(bare.get("metadata").is_none(), "{bare}");
    }

    #[test]
    fn output_round_trips_as_typed_items() {
        // This is what lets a bare chain be reconstructed from its stream without
        // reaching into a JSON map by string key.
        let object = ResponseObject::new(&record(), vec![ResponseItem::assistant_text("out")]);
        let json = serde_json::to_string(&object).unwrap();
        let parsed: ResponseObject = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, object);
        assert_eq!(parsed.output, vec![ResponseItem::assistant_text("out")]);
    }

    #[test]
    fn a_stub_is_enough_to_end_a_stream() {
        let id = ResponseId::new(NodeTag::parse("n1").unwrap());
        let json = serde_json::to_value(ResponseObject::terminal_stub(
            &id,
            ResponseStatus::Failed,
        ))
        .unwrap();
        assert_eq!(json["id"], id.to_string());
        assert_eq!(json["status"], "failed");
        assert_eq!(json["object"], "response");
    }
}
