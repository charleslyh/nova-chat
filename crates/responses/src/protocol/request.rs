//! Create-response request envelope — the full accepted parameter set (D22).
//!
//! `input` and `tool_choice` are genuine unions in the upstream protocol. They
//! are deserialised with hand-written visitors rather than `#[serde(untagged)]`
//! because `untagged` reports "data did not match any variant" and **discards
//! the real error** — a malformed item deep inside an input array would surface
//! as an unhelpful top-level failure. The visitors dispatch on the JSON kind
//! first, then let the inner error propagate verbatim.

use std::collections::BTreeMap;
use std::fmt;

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use thiserror::Error;

use super::item::{ItemViolation, ResponseItem};
use super::limits::{InputLimits, LimitViolation};

/// Upstream metadata constraints.
pub const MAX_METADATA_ENTRIES: usize = 16;
pub const MAX_METADATA_KEY_BYTES: usize = 64;
pub const MAX_METADATA_VALUE_BYTES: usize = 512;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateResponseRequest {
    pub model: String,
    pub input: ResponseInput,

    /// A system/developer message inserted at the front of the context.
    ///
    /// **Not an item**: it never appears in the item list, is echoed on the
    /// response object, and — critically — is **not inherited across turns**.
    /// See INV-49.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,

    /// Persist input/output items for later chaining. Defaults to `true`.
    #[serde(default = "default_true")]
    pub store: bool,

    #[serde(default)]
    pub stream: bool,

    #[serde(default)]
    pub background: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_response_id: Option<String>,

    /// Conversation to continue, and whose tail pointer this response advances
    /// once it completes (D27).
    ///
    /// Mutually exclusive with `previous_response_id`: both name the context to
    /// inherit, and honouring one while ignoring the other would be a silent
    /// choice made on the caller's behalf.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation: Option<ConversationRef>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<BTreeMap<String, String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<Tool>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoice>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
}

fn default_true() -> bool {
    true
}

/// `conversation` accepts either a bare id string or `{"id": "conv_…"}`.
///
/// Both forms are upstream's, not ours, so both are preserved on the wire rather
/// than normalised on the way in — a request that round-trips differently from
/// how it arrived is a request the caller cannot recognise. Consumers use
/// [`ConversationRef::id`] and never branch on the shape.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ConversationRef {
    Id(String),
    Object { id: String },
}

impl ConversationRef {
    pub fn id(&self) -> &str {
        match self {
            ConversationRef::Id(id) | ConversationRef::Object { id } => id,
        }
    }
}

impl<'de> Deserialize<'de> for ConversationRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RefVisitor;

        impl<'de> Visitor<'de> for RefVisitor {
            type Value = ConversationRef;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a conversation id string or an object with an `id` field")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(ConversationRef::Id(v.to_string()))
            }

            fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
                Ok(ConversationRef::Id(v))
            }

            /// Dispatching on the JSON kind first, then letting the inner error
            /// through, is why this is hand-written: `untagged` would report
            /// "data did not match any variant" and discard which field was
            /// wrong (see the module header).
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut id: Option<String> = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "id" => {
                            if id.is_some() {
                                return Err(de::Error::duplicate_field("id"));
                            }
                            id = Some(map.next_value()?);
                        }
                        other => {
                            return Err(de::Error::unknown_field(other, &["id"]));
                        }
                    }
                }
                let id = id.ok_or_else(|| de::Error::missing_field("id"))?;
                Ok(ConversationRef::Object { id })
            }
        }

        deserializer.deserialize_any(RefVisitor)
    }
}

/// Validate a metadata map against the upstream limits.
///
/// Shared by the create-response request and the conversation endpoints because
/// upstream applies the same numbers to both. One implementation means the two
/// cannot drift into disagreeing about what a valid key is.
pub fn validate_metadata(
    metadata: &BTreeMap<String, String>,
) -> Result<(), RequestViolation> {
    if metadata.len() > MAX_METADATA_ENTRIES {
        return Err(RequestViolation::TooManyMetadataEntries {
            actual: metadata.len(),
            max: MAX_METADATA_ENTRIES,
        });
    }
    for (key, value) in metadata {
        if key.len() > MAX_METADATA_KEY_BYTES {
            return Err(RequestViolation::MetadataKeyTooLong {
                key: key.clone(),
                max: MAX_METADATA_KEY_BYTES,
            });
        }
        if value.len() > MAX_METADATA_VALUE_BYTES {
            return Err(RequestViolation::MetadataValueTooLong {
                key: key.clone(),
                max: MAX_METADATA_VALUE_BYTES,
            });
        }
    }
    Ok(())
}

/// `input` accepts either a bare string (shorthand for a single user message)
/// or an explicit item array.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ResponseInput {
    Text(String),
    Items(Vec<ResponseItem>),
}

impl ResponseInput {
    /// Normalise to items. The string shorthand becomes one user message.
    pub fn to_items(&self) -> Vec<ResponseItem> {
        match self {
            ResponseInput::Text(text) => vec![ResponseItem::user_text(text.clone())],
            ResponseInput::Items(items) => items.clone(),
        }
    }
}

impl<'de> Deserialize<'de> for ResponseInput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct InputVisitor;

        impl<'de> Visitor<'de> for InputVisitor {
            type Value = ResponseInput;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a string or an array of response items")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(ResponseInput::Text(v.to_string()))
            }

            fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
                Ok(ResponseInput::Text(v))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut items = Vec::new();
                // Errors from individual items propagate unchanged, preserving
                // the offending item's index and field name.
                while let Some(item) = seq.next_element::<ResponseItem>()? {
                    items.push(item);
                }
                Ok(ResponseInput::Items(items))
            }
        }

        deserializer.deserialize_any(InputVisitor)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Tool {
    Function {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        description: Option<String>,
        /// JSON Schema; opaque to this service but depth-checked (SEC-7).
        parameters: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        strict: Option<bool>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolChoiceMode {
    Auto,
    None,
    Required,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ToolChoice {
    Mode(ToolChoiceMode),
    Function { name: String },
}

impl<'de> Deserialize<'de> for ToolChoice {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ChoiceVisitor;

        impl<'de> Visitor<'de> for ChoiceVisitor {
            type Value = ToolChoice;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(r#""auto" | "none" | "required" | {"type":"function","name":"…"}"#)
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                match v {
                    "auto" => Ok(ToolChoice::Mode(ToolChoiceMode::Auto)),
                    "none" => Ok(ToolChoice::Mode(ToolChoiceMode::None)),
                    "required" => Ok(ToolChoice::Mode(ToolChoiceMode::Required)),
                    other => Err(E::unknown_variant(other, &["auto", "none", "required"])),
                }
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut kind: Option<String> = None;
                let mut name: Option<String> = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "type" => kind = Some(map.next_value()?),
                        "name" => name = Some(map.next_value()?),
                        other => return Err(de::Error::unknown_field(other, &["type", "name"])),
                    }
                }
                match kind.as_deref() {
                    Some("function") => {
                        let name = name.ok_or_else(|| de::Error::missing_field("name"))?;
                        Ok(ToolChoice::Function { name })
                    }
                    Some(other) => Err(de::Error::unknown_variant(other, &["function"])),
                    None => Err(de::Error::missing_field("type")),
                }
            }
        }

        deserializer.deserialize_any(ChoiceVisitor)
    }
}

#[derive(Debug, Clone, PartialEq, Error)]
pub enum RequestViolation {
    #[error("model must not be empty")]
    EmptyModel,
    #[error("input invalid: {0}")]
    Limits(#[from] LimitViolation),
    #[error("item {index} invalid: {source}")]
    Item {
        index: usize,
        #[source]
        source: ItemViolation,
    },
    #[error("instructions exceed {max} bytes")]
    InstructionsTooLong { max: usize },
    #[error("previous_response_id must not be empty")]
    EmptyPreviousId,
    #[error("conversation id must not be empty")]
    EmptyConversationId,
    /// Both name the context to inherit. Upstream's own documentation does not
    /// state whether it rejects the combination, so this is our call, taken the
    /// way the rest of the service takes such calls: fail loudly rather than pick
    /// one silently (see `docs/design/07-conversations.md`).
    #[error("previous_response_id and conversation must not both be set")]
    PreviousIdAndConversation,
    #[error("max_output_tokens must be greater than zero")]
    ZeroMaxOutputTokens,
    #[error("temperature must be within [0, 2]")]
    TemperatureOutOfRange,
    #[error("top_p must be within [0, 1]")]
    TopPOutOfRange,
    #[error("metadata has {actual} entries, limit is {max}")]
    TooManyMetadataEntries { actual: usize, max: usize },
    #[error("metadata key `{key}` exceeds {max} bytes")]
    MetadataKeyTooLong { key: String, max: usize },
    #[error("metadata value for `{key}` exceeds {max} bytes")]
    MetadataValueTooLong { key: String, max: usize },
    #[error("tool `{name}` parameters invalid: {source}")]
    ToolParameters {
        name: String,
        #[source]
        source: LimitViolation,
    },
    /// `stream` and `background` are both about delivery; combining them is
    /// ambiguous, so it is rejected rather than silently preferring one.
    #[error("stream and background must not both be set")]
    StreamAndBackground,
}

pub const MAX_INSTRUCTIONS_BYTES: usize = 32 * 1024;

impl CreateResponseRequest {
    /// Full semantic validation. Structural rejection already happened during
    /// deserialisation; this covers value ranges and cross-field rules.
    ///
    /// Returns the normalised input items on success.
    pub fn validate(&self, limits: &InputLimits) -> Result<Vec<ResponseItem>, RequestViolation> {
        if self.model.trim().is_empty() {
            return Err(RequestViolation::EmptyModel);
        }
        if self.stream && self.background {
            return Err(RequestViolation::StreamAndBackground);
        }
        if let Some(instructions) = &self.instructions {
            if instructions.len() > MAX_INSTRUCTIONS_BYTES {
                return Err(RequestViolation::InstructionsTooLong {
                    max: MAX_INSTRUCTIONS_BYTES,
                });
            }
        }
        if let Some(previous) = &self.previous_response_id {
            if previous.trim().is_empty() {
                return Err(RequestViolation::EmptyPreviousId);
            }
        }
        if let Some(conversation) = &self.conversation {
            if conversation.id().trim().is_empty() {
                return Err(RequestViolation::EmptyConversationId);
            }
            if self.previous_response_id.is_some() {
                return Err(RequestViolation::PreviousIdAndConversation);
            }
        }
        if let Some(0) = self.max_output_tokens {
            return Err(RequestViolation::ZeroMaxOutputTokens);
        }
        if let Some(t) = self.temperature {
            if !(0.0..=2.0).contains(&t) || t.is_nan() {
                return Err(RequestViolation::TemperatureOutOfRange);
            }
        }
        if let Some(p) = self.top_p {
            if !(0.0..=1.0).contains(&p) || p.is_nan() {
                return Err(RequestViolation::TopPOutOfRange);
            }
        }
        if let Some(metadata) = &self.metadata {
            validate_metadata(metadata)?;
        }
        if let Some(tools) = &self.tools {
            for tool in tools {
                let Tool::Function {
                    name, parameters, ..
                } = tool;
                limits.validate_depth(parameters).map_err(|source| {
                    RequestViolation::ToolParameters {
                        name: name.clone(),
                        source,
                    }
                })?;
            }
        }

        let items = self.input.to_items();
        limits.validate_items(&items)?;
        for (index, item) in items.iter().enumerate() {
            item.validate()
                .map_err(|source| RequestViolation::Item { index, source })?;
        }
        Ok(items)
    }
}

/// Detect deliberately unsupported upstream fields *before* deserialisation so
/// the caller gets a specific remedy instead of a generic "unknown field".
///
/// Returns `(field, explanation)` for the first match.
pub fn preflight_unsupported(raw: &Value) -> Option<(&'static str, &'static str)> {
    let obj = raw.as_object()?;
    super::EXPLICITLY_UNSUPPORTED_FIELDS
        .iter()
        .find(|(field, _)| obj.contains_key(*field))
        .copied()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ContentPart, Role};

    fn minimal(json: &str) -> Result<CreateResponseRequest, serde_json::Error> {
        serde_json::from_str(json)
    }

    #[test]
    fn store_defaults_to_true() {
        let req = minimal(r#"{"model":"m","input":"hi"}"#).unwrap();
        assert!(req.store, "store must default to true (D20 ②)");
        assert!(!req.stream);
        assert!(!req.background);
    }

    #[test]
    fn accepts_string_shorthand_and_expands_to_user_message() {
        let req = minimal(r#"{"model":"m","input":"hi"}"#).unwrap();
        assert_eq!(req.input, ResponseInput::Text("hi".into()));
        assert_eq!(
            req.input.to_items(),
            vec![ResponseItem::Message {
                role: Role::User,
                content: vec![ContentPart::InputText { text: "hi".into() }],
                id: None,
                status: None,
            }]
        );
    }

    #[test]
    fn accepts_item_array() {
        let req = minimal(
            r#"{"model":"m","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"a"}]}]}"#,
        )
        .unwrap();
        assert_eq!(req.input.to_items().len(), 1);
    }

    #[test]
    fn rejects_unknown_request_field() {
        let err = minimal(r#"{"model":"m","input":"hi","truncation":"auto"}"#)
            .expect_err("unknown field must be rejected");
        assert!(err.to_string().contains("truncation"), "{err}");
    }

    #[test]
    fn malformed_item_error_names_the_real_problem() {
        // This is the payoff for not using #[serde(untagged)] on `input`:
        // the error must point at the item type, not at "no variant matched".
        let err = minimal(r#"{"model":"m","input":[{"type":"item_reference","id":"x"}]}"#)
            .expect_err("must fail");
        let msg = err.to_string();
        assert!(msg.contains("item_reference"), "unhelpful error: {msg}");
        assert!(
            !msg.contains("did not match any variant"),
            "untagged fallback leaked: {msg}"
        );
    }

    #[test]
    fn preflight_no_longer_rejects_conversation() {
        // The field is inside the subset now (D27). Leaving it on the rejection
        // list would have made the new feature unreachable behind its own
        // pre-flight check.
        let raw: Value =
            serde_json::from_str(r#"{"model":"m","input":"a","conversation":"conv_1"}"#).unwrap();
        assert_eq!(preflight_unsupported(&raw), None);
    }

    #[test]
    fn conversation_accepts_both_upstream_shapes_and_null() {
        let req = minimal(r#"{"model":"m","input":"a","conversation":"conv_1"}"#).unwrap();
        assert_eq!(
            req.conversation,
            Some(ConversationRef::Id("conv_1".into()))
        );
        assert_eq!(req.conversation.as_ref().map(ConversationRef::id), Some("conv_1"));

        let req = minimal(r#"{"model":"m","input":"a","conversation":{"id":"conv_2"}}"#).unwrap();
        assert_eq!(
            req.conversation,
            Some(ConversationRef::Object { id: "conv_2".into() })
        );
        assert_eq!(req.conversation.as_ref().map(ConversationRef::id), Some("conv_2"));

        // `null` is an accepted upstream spelling of "no conversation".
        let req = minimal(r#"{"model":"m","input":"a","conversation":null}"#).unwrap();
        assert_eq!(req.conversation, None);
    }

    #[test]
    fn conversation_shape_is_preserved_across_a_round_trip() {
        for json in [
            r#"{"model":"m","input":"a","conversation":"conv_1"}"#,
            r#"{"model":"m","input":"a","conversation":{"id":"conv_1"}}"#,
        ] {
            let req = minimal(json).unwrap();
            let again: CreateResponseRequest =
                serde_json::from_str(&serde_json::to_string(&req).unwrap()).unwrap();
            assert_eq!(again.conversation, req.conversation, "{json}");
        }
    }

    #[test]
    fn malformed_conversation_objects_report_the_real_problem() {
        // The hand-written visitor exists so these say what is wrong instead of
        // "data did not match any variant".
        let err = minimal(r#"{"model":"m","input":"a","conversation":{}}"#).unwrap_err();
        assert!(err.to_string().contains("id"), "unhelpful error: {err}");

        let err = minimal(r#"{"model":"m","input":"a","conversation":{"ref":"conv_1"}}"#)
            .unwrap_err();
        assert!(err.to_string().contains("ref"), "unhelpful error: {err}");

        let err = minimal(r#"{"model":"m","input":"a","conversation":7}"#).unwrap_err();
        assert!(
            !err.to_string().contains("did not match any variant"),
            "untagged fallback leaked: {err}"
        );
    }

    #[test]
    fn conversation_and_previous_response_id_are_mutually_exclusive() {
        let req = minimal(
            r#"{"model":"m","input":"a","conversation":"conv_1","previous_response_id":"resp_n1_x"}"#,
        )
        .unwrap();
        assert_eq!(
            req.validate(&InputLimits::default()),
            Err(RequestViolation::PreviousIdAndConversation)
        );

        // Either one alone is fine.
        assert!(minimal(r#"{"model":"m","input":"a","conversation":"conv_1"}"#)
            .unwrap()
            .validate(&InputLimits::default())
            .is_ok());
    }

    #[test]
    fn empty_conversation_ids_are_rejected_in_both_shapes() {
        for json in [
            r#"{"model":"m","input":"a","conversation":""}"#,
            r#"{"model":"m","input":"a","conversation":{"id":"   "}}"#,
        ] {
            assert_eq!(
                minimal(json).unwrap().validate(&InputLimits::default()),
                Err(RequestViolation::EmptyConversationId),
                "{json}"
            );
        }
    }

    #[test]
    fn preflight_flags_context_management_and_prompt() {
        for field in ["context_management", "prompt"] {
            let raw: Value =
                serde_json::from_str(&format!(r#"{{"model":"m","input":"a","{field}":{{}}}}"#))
                    .unwrap();
            assert_eq!(preflight_unsupported(&raw).unwrap().0, field);
        }
    }

    #[test]
    fn tool_choice_accepts_both_shapes() {
        let mode: ToolChoice = serde_json::from_str(r#""required""#).unwrap();
        assert_eq!(mode, ToolChoice::Mode(ToolChoiceMode::Required));
        let func: ToolChoice = serde_json::from_str(r#"{"type":"function","name":"f"}"#).unwrap();
        assert_eq!(func, ToolChoice::Function { name: "f".into() });
        assert!(serde_json::from_str::<ToolChoice>(r#""whatever""#).is_err());
        assert!(serde_json::from_str::<ToolChoice>(r#"{"type":"mcp","name":"f"}"#).is_err());
    }

    #[test]
    fn rejects_hosted_tool_types() {
        assert!(minimal(r#"{"model":"m","input":"a","tools":[{"type":"web_search"}]}"#).is_err());
        assert!(minimal(
            r#"{"model":"m","input":"a","tools":[{"type":"function","name":"f","parameters":{}}]}"#
        )
        .is_ok());
    }

    #[test]
    fn validates_ranges() {
        let limits = InputLimits::default();
        let bad_temp = minimal(r#"{"model":"m","input":"a","temperature":3}"#).unwrap();
        assert_eq!(
            bad_temp.validate(&limits),
            Err(RequestViolation::TemperatureOutOfRange)
        );
        let bad_top = minimal(r#"{"model":"m","input":"a","top_p":1.5}"#).unwrap();
        assert_eq!(
            bad_top.validate(&limits),
            Err(RequestViolation::TopPOutOfRange)
        );
        let zero = minimal(r#"{"model":"m","input":"a","max_output_tokens":0}"#).unwrap();
        assert_eq!(
            zero.validate(&limits),
            Err(RequestViolation::ZeroMaxOutputTokens)
        );
        let empty_model = minimal(r#"{"model":"  ","input":"a"}"#).unwrap();
        assert_eq!(empty_model.validate(&limits), Err(RequestViolation::EmptyModel));
    }

    #[test]
    fn rejects_stream_and_background_together() {
        let req = minimal(r#"{"model":"m","input":"a","stream":true,"background":true}"#).unwrap();
        assert_eq!(
            req.validate(&InputLimits::default()),
            Err(RequestViolation::StreamAndBackground)
        );
    }

    #[test]
    fn rejects_internal_url_during_validation() {
        let req = minimal(
            r#"{"model":"m","input":[{"type":"message","role":"user","content":[{"type":"input_image","image_url":"https://10.0.0.1/x.png"}]}]}"#,
        )
        .unwrap();
        let err = req.validate(&InputLimits::default()).expect_err("must reject");
        assert!(matches!(err, RequestViolation::Item { index: 0, .. }), "{err}");
    }

    #[test]
    fn validate_accepts_well_formed_request() {
        let req = minimal(
            r#"{"model":"m","input":"hi","instructions":"be brief","temperature":0.7,"metadata":{"k":"v"}}"#,
        )
        .unwrap();
        let items = req.validate(&InputLimits::default()).unwrap();
        assert_eq!(items.len(), 1);
    }
}
