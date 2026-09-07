//! Upstream-compatible conversation: a **pointer to the tail of a response
//! chain** (D27).
//!
//! Upstream describes a conversation as a container whose items get prepended to
//! the next request's input. Here it holds no items at all — it holds
//! `last_response_id`, and the chain hanging off that id already *is* the
//! history, because every response carries a flat copy of everything before it
//! (D24).
//!
//! The two are observationally equivalent for a caller: passing the same
//! conversation id on each request inherits the whole history either way. What
//! the pointer form buys is that there is exactly one place conversation content
//! lives. A container would be a second copy of the same items, with the usual
//! consequence — two stores that can disagree.
//!
//! What it costs is the `items` sub-resource, which is not implemented: no
//! append, list, retrieve or delete of individual items. Reading history goes
//! through the session layer's transcript endpoint instead, which returns the
//! whole thing in one call rather than obliging every caller to paginate.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::context::ResponseStatus;
use crate::ids::{ConversationId, ResponseId, TenantId};

/// A conversation record.
///
/// `metadata` reuses the request-side limits (16 entries, 64-byte keys,
/// 512-byte values) rather than defining its own, because upstream applies the
/// same numbers to both and one definition cannot drift from itself.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Conversation {
    pub id: ConversationId,
    pub tenant_id: TenantId,

    /// Tail of the response chain, i.e. the context the next generation
    /// inherits. `None` until the first turn completes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_response_id: Option<ResponseId>,

    /// The response currently in flight for this conversation, if any (D28).
    ///
    /// This is the mutual-exclusion marker: one conversation admits one
    /// in-flight turn at a time. `Some(id)` means busy; `None` means idle. It is
    /// **not** exposed on the official conversation object — it is an internal
    /// serialisation gate reached through the store's `acquire_active` /
    /// `release_active` compare-and-set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_response_id: Option<ResponseId>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,

    pub created_at_ms: u64,
}

impl Conversation {
    /// A fresh conversation with no chain behind it yet.
    pub fn new(
        id: ConversationId,
        tenant_id: TenantId,
        metadata: BTreeMap<String, String>,
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

    /// Whether any turn has completed against this conversation.
    pub fn is_empty(&self) -> bool {
        self.last_response_id.is_none()
    }
}

/// The closed set of conversation events (D28).
///
/// Turn boundaries are emitted atomically with the mutual-exclusion marker
/// transition (see `ConversationStore::acquire_active` / `release_active`), so an
/// unpaired `TurnStarted` *is* the "busy" state. There is no separate
/// lock-acquired / lock-released pair — two events for one fact would be two
/// things to keep consistent.
///
/// Every name is prefixed `conversation.`, keeping this stream in a namespace of
/// its own, clear of upstream's `response.*` and `conversation.*` (the CRUD
/// resource) namespaces.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ConversationEventKind {
    /// A turn began. Emitted atomically with occupying the in-flight marker.
    #[serde(rename = "conversation.turn_started")]
    TurnStarted { response_id: ResponseId },

    /// A turn reached a terminal status. Emitted atomically with releasing the
    /// marker, on **every** terminal path.
    #[serde(rename = "conversation.turn_completed")]
    TurnCompleted {
        response_id: ResponseId,
        status: ResponseStatus,
    },

    /// A response record was deleted, so every device can drop the bubble.
    #[serde(rename = "conversation.response_deleted")]
    ResponseDeleted { response_id: ResponseId },

    /// Business-side event, ordered in the same sequence space. The **only** open
    /// variant; the payload is opaque and bounded. Never enters model context.
    #[serde(rename = "conversation.business")]
    Business { kind: String, payload: Value },
}

impl ConversationEventKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ConversationEventKind::TurnStarted { .. } => "conversation.turn_started",
            ConversationEventKind::TurnCompleted { .. } => "conversation.turn_completed",
            ConversationEventKind::ResponseDeleted { .. } => "conversation.response_deleted",
            ConversationEventKind::Business { .. } => "conversation.business",
        }
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

    fn conversation() -> Conversation {
        Conversation::new(
            ConversationId::new(),
            TenantId::parse("t1").unwrap(),
            BTreeMap::new(),
            7,
        )
    }

    #[test]
    fn a_new_conversation_points_nowhere() {
        let c = conversation();
        assert!(c.is_empty());
        assert_eq!(c.last_response_id, None);
        assert_eq!(c.created_at_ms, 7);
    }

    #[test]
    fn the_record_holds_a_pointer_and_no_items() {
        // Structural guard for the single-source-of-truth rule: if an `items`
        // field is ever added here, conversation content would live in two
        // places at once.
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
        c.metadata.insert("topic".into(), "demo".into());
        let json = serde_json::to_string(&c).unwrap();
        assert_eq!(serde_json::from_str::<Conversation>(&json).unwrap(), c);

        c.last_response_id = Some(ResponseId::new(crate::ids::NodeTag::parse("n1").unwrap()));
        assert!(!c.is_empty());
        let json = serde_json::to_string(&c).unwrap();
        assert_eq!(serde_json::from_str::<Conversation>(&json).unwrap(), c);
    }
}
