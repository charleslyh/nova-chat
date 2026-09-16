//! Upstream-compatible conversation: a **pointer to the tail of a response
//! chain** (D27), and the long-term home of the dialogue's content (D30).
//!
//! Upstream describes a conversation as a container whose items get prepended to
//! the next request's input. Here it holds no items *inline* — it holds
//! `last_response_id` plus a materialised snapshot the store owns, so there is
//! exactly one place conversation content lives. A container of items beside the
//! snapshot would be a second copy, with the usual consequence: two stores that
//! can disagree.
//!
//! What it costs is the `items` sub-resource, which is not implemented: no
//! append, list, retrieve or delete of individual items. Reading history goes
//! through the transcript endpoint instead, which returns the whole thing in one
//! call rather than obliging every caller to paginate.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use uuid::Uuid;

use crate::identity::{IdError, TenantId};
use crate::protocol::{MetadataValue, ResponseItem};
use crate::response::{ResponseId, ResponseStatus};
use crate::usage::Usage;

/// `conv_{uuid}`.
///
/// Carries **no node tag**, unlike [`ResponseId`]. Nothing about a conversation is
/// node-local: there is no in-flight buffer to route a subscription to, so
/// embedding a routing hint would add an address-forgery surface (SEC-5) while
/// buying nothing. It lives in the shared store and any node can serve it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConversationId(Uuid);

impl ConversationId {
    pub const PREFIX: &'static str = "conv_";

    fn err() -> IdError {
        IdError::new("conversation id", "conv_<uuid>")
    }

    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// The only validator: `FromStr` and `Deserialize` both route through it, so a
    /// malformed stored id fails loudly on read.
    pub fn parse(raw: &str) -> Result<Self, IdError> {
        let rest = raw.strip_prefix(Self::PREFIX).ok_or_else(Self::err)?;
        let uuid = Uuid::parse_str(rest).map_err(|_| Self::err())?;
        Ok(Self(uuid))
    }

    pub fn uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for ConversationId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ConversationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::PREFIX, self.0)
    }
}

impl FromStr for ConversationId {
    type Err = IdError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Serialize for ConversationId {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ConversationId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        Self::parse(&raw).map_err(de::Error::custom)
    }
}

/// A conversation record.
///
/// `metadata` reuses the request-side limits rather than defining its own,
/// because upstream applies the same numbers to both and one definition cannot
/// drift from itself.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Conversation {
    pub id: ConversationId,
    pub tenant_id: TenantId,

    /// Tail of the response chain, i.e. the context the next generation inherits.
    /// `None` until the first turn completes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_response_id: Option<ResponseId>,

    /// The response currently in flight for this conversation, if any (D28).
    ///
    /// This is the mutual-exclusion marker: one conversation admits one in-flight
    /// turn at a time. `Some(id)` means busy; `None` means idle. It is **not**
    /// exposed on the official conversation object — it is an internal
    /// serialisation gate reached through the store's compare-and-set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_response_id: Option<ResponseId>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, MetadataValue>,

    pub created_at_ms: u64,
}

impl Conversation {
    /// A fresh conversation with no chain behind it yet.
    pub fn new(
        id: ConversationId,
        tenant_id: TenantId,
        metadata: BTreeMap<String, MetadataValue>,
        created_at_ms: u64,
    ) -> Self {
        Self {
            id,
            tenant_id,
            last_response_id: None,
            active_response_id: None,
            metadata,
            created_at_ms,
        }
    }
}

/// The durable content one turn commits to a conversation snapshot at terminal
/// time (D30).
///
/// A value object rather than nine arguments whose items, usage and status are
/// really one fact. It lives here, with the conversation it is committed to,
/// rather than in the port module: ports own traits and their failures, not the
/// domain values that travel through them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnCommit {
    pub input_items: Vec<ResponseItem>,
    pub output_items: Vec<ResponseItem>,
    /// Render-only, placed immediately before the output block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,
    pub usage: Usage,
    pub status: ResponseStatus,
}

/// The closed set of conversation events (D28).
///
/// Turn boundaries are emitted atomically with the mutual-exclusion marker
/// transition (see [`crate::ports::TurnLock`]), so an unpaired `TurnStarted` *is*
/// the "busy" state. There is no separate lock-acquired / lock-released pair —
/// two events for one fact would be two things to keep consistent.
///
/// Every name is prefixed `conversation.`, keeping this stream in a namespace of
/// its own, clear of the `response.*` namespace.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, strum::IntoStaticStr)]
#[serde(tag = "type")]
pub enum ConversationEventKind {
    /// A turn began. Emitted atomically with occupying the in-flight marker.
    #[serde(rename = "conversation.turn_started")]
    #[strum(serialize = "conversation.turn_started")]
    TurnStarted { response_id: ResponseId },

    /// A turn reached a terminal status. Emitted atomically with releasing the
    /// marker, on **every** terminal path.
    #[serde(rename = "conversation.turn_completed")]
    #[strum(serialize = "conversation.turn_completed")]
    TurnCompleted {
        response_id: ResponseId,
        status: ResponseStatus,
    },

    /// A response record was deleted, so every device can drop the bubble.
    #[serde(rename = "conversation.response_deleted")]
    #[strum(serialize = "conversation.response_deleted")]
    ResponseDeleted { response_id: ResponseId },

    /// Business-side event, ordered in the same sequence space. The **only** open
    /// variant; the payload is opaque and bounded. Never enters model context.
    #[serde(rename = "conversation.business")]
    #[strum(serialize = "conversation.business")]
    Business { kind: String, payload: Value },
}

impl ConversationEventKind {
    /// The wire name, as used for the SSE event name.
    ///
    /// serde gives no runtime access to the variant name of a data-carrying enum,
    /// so the literal appears twice: once for the tag, once for strum. A test
    /// below pins them together, which is the enforcement — not a convention.
    pub fn as_str(&self) -> &'static str {
        self.into()
    }

    pub fn response_id(&self) -> Option<&ResponseId> {
        match self {
            ConversationEventKind::TurnStarted { response_id }
            | ConversationEventKind::TurnCompleted { response_id, .. }
            | ConversationEventKind::ResponseDeleted { response_id } => Some(response_id),
            ConversationEventKind::Business { .. } => None,
        }
    }
}

/// One entry in a conversation's event stream.
///
/// `seq` is 0-based and contiguous per conversation, matching the per-response
/// event log so both streams are read with one cursor rule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConversationEvent {
    pub conversation_id: ConversationId,
    pub seq: u64,
    pub kind: ConversationEventKind,
    pub ts_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::NodeTag;

    fn conversation() -> Conversation {
        Conversation::new(
            ConversationId::new(),
            TenantId::parse("t1").unwrap(),
            BTreeMap::new(),
            7,
        )
    }

    fn response_id() -> ResponseId {
        ResponseId::new(NodeTag::parse("n1").unwrap())
    }

    #[test]
    fn conversation_id_round_trips() {
        let c = ConversationId::new();
        let text = c.to_string();
        assert!(text.starts_with("conv_"), "{text}");
        assert_eq!(ConversationId::parse(&text).unwrap(), c);
        assert_eq!(
            serde_json::from_str::<ConversationId>(&serde_json::to_string(&c).unwrap()).unwrap(),
            c
        );
    }

    #[test]
    fn conversation_id_carries_no_node_tag() {
        let uuid = Uuid::new_v4();
        assert!(ConversationId::parse(&format!("conv_node-a_{uuid}")).is_err());
    }

    #[test]
    fn rejects_malformed_conversation_ids() {
        let uuid = Uuid::new_v4().to_string();
        for bad in ["", "abc", "conv", "conv_", "conv_not-a-uuid", &uuid] {
            let err = ConversationId::parse(bad).expect_err("must be rejected");
            assert_eq!(err.kind, "conversation id", "`{bad}`");
        }
        // Path traversal attempts inside the id.
        assert!(ConversationId::parse("conv_../../etc").is_err());
    }

    #[test]
    fn a_new_conversation_points_nowhere() {
        let c = conversation();
        assert_eq!(c.last_response_id, None);
        assert_eq!(c.active_response_id, None);
        assert_eq!(c.created_at_ms, 7);
    }

    #[test]
    fn the_record_holds_a_pointer_and_no_items() {
        // Structural guard for the single-source-of-truth rule: if an `items`
        // field is ever added here, conversation content would live in two places
        // at once — the snapshot the store owns, and this.
        let value = serde_json::to_value(conversation()).unwrap();
        let keys: Vec<_> = value.as_object().unwrap().keys().cloned().collect();
        assert!(
            !keys.iter().any(|k| k == "items"),
            "conversation must stay a pointer, not a container: {keys:?}"
        );
    }

    #[test]
    fn round_trips_with_and_without_a_tail() {
        let mut c = conversation();
        c.metadata
            .insert("topic".into(), MetadataValue::String("demo".into()));
        let json = serde_json::to_string(&c).unwrap();
        assert_eq!(serde_json::from_str::<Conversation>(&json).unwrap(), c);

        c.last_response_id = Some(response_id());
        let json = serde_json::to_string(&c).unwrap();
        assert_eq!(serde_json::from_str::<Conversation>(&json).unwrap(), c);
    }

    #[test]
    fn the_sse_name_and_the_serialised_tag_cannot_drift() {
        // `as_str` feeds the SSE event name and the tag feeds the payload. A
        // subscriber filters on the former and matches on the latter, so a
        // mismatch would make an event unroutable while looking correct.
        for kind in [
            ConversationEventKind::TurnStarted {
                response_id: response_id(),
            },
            ConversationEventKind::TurnCompleted {
                response_id: response_id(),
                status: ResponseStatus::Completed,
            },
            ConversationEventKind::ResponseDeleted {
                response_id: response_id(),
            },
            ConversationEventKind::Business {
                kind: "k".into(),
                payload: Value::Null,
            },
        ] {
            let json = serde_json::to_value(&kind).unwrap();
            assert_eq!(json["type"], kind.as_str(), "{kind:?}");
            assert!(kind.as_str().starts_with("conversation."), "{kind:?}");
            assert_eq!(
                serde_json::from_value::<ConversationEventKind>(json).unwrap(),
                kind
            );
        }
    }
}
