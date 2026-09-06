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
            metadata,
            created_at_ms,
        }
    }

    /// Whether any turn has completed against this conversation.
    pub fn is_empty(&self) -> bool {
        self.last_response_id.is_none()
    }
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
