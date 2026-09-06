//! Self-hosted session layer: the event stream, the single-turn lock and the
//! business events (D26).
//!
//! This layer exists because no upstream protocol covers multi-device fan-out
//! or interleaving business events with the conversation timeline. It is
//! deliberately **not** a second home for conversation content:
//!
//! > Events carry references, never content.
//!
//! The single source of truth for what was said stays in the materialised
//! snapshot held by the context store (D24). A session event says *that* a turn
//! started and *which* response it was; reading the words means following that
//! reference. Copying content in would create a second copy to keep in sync,
//! which is exactly the failure mode the materialised snapshot was designed to
//! avoid.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::context::ResponseStatus;
use crate::ids::{ConversationId, ResponseId, SessionId, TenantId};

/// Whether a turn is currently in flight for this session.
///
/// Derivable from the event stream (a `TurnStarted` without its matching
/// `TurnCompleted`), but stored explicitly because that is what makes the
/// compare-and-set in `begin_turn` a single atomic operation instead of a scan.
/// Clients that only need the current state read it from the session object
/// rather than folding the stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum LockState {
    Idle,
    Busy { response_id: ResponseId },
}

impl LockState {
    pub fn is_busy(&self) -> bool {
        matches!(self, LockState::Busy { .. })
    }

    /// The response holding the lock, if any.
    pub fn holder(&self) -> Option<&ResponseId> {
        match self {
            LockState::Idle => None,
            LockState::Busy { response_id } => Some(response_id),
        }
    }
}

/// A session: one conversation plus one event stream.
///
/// The `conversation_id` is bound at creation and never changes. That binding is
/// what lets the session layer stay out of context assembly entirely — the
/// chain lives in the compatibility layer, and the session only ever needs the
/// pointer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Session {
    pub id: SessionId,
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub lock_state: LockState,
    pub created_at_ms: u64,
}

impl Session {
    /// A fresh, idle session bound to `conversation_id`.
    pub fn new(
        id: SessionId,
        tenant_id: TenantId,
        conversation_id: ConversationId,
        created_at_ms: u64,
    ) -> Self {
        Self {
            id,
            tenant_id,
            conversation_id,
            lock_state: LockState::Idle,
            created_at_ms,
        }
    }
}

/// The closed set of session events, plus one deliberately open variant.
///
/// Only five variants, and each one records a fact no other variant records.
/// There is no separate `LockAcquired` / `LockReleased` pair: those would state
/// the same fact as `TurnStarted` / `TurnCompleted`, and two events for one fact
/// means two things to keep consistent. The lock is *implied* by an unpaired
/// `TurnStarted`, and read directly from [`LockState`] when only the current
/// value is wanted.
/// Every name is prefixed `session.`, keeping this stream in a namespace of its
/// own. Upstream owns `response.*` and `conversation.*`; a self-hosted event
/// called `response.deleted` would sit in the middle of a namespace we do not
/// control and collide the first time upstream defines it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SessionEventKind {
    /// Always sequence 0, so a subscriber starting from the beginning can tell
    /// "no events yet" from "stream begins here".
    #[serde(rename = "session.created")]
    SessionCreated,

    /// A turn began. Emitted atomically with acquiring the lock.
    #[serde(rename = "session.turn_started")]
    TurnStarted { response_id: ResponseId },

    /// A turn reached a terminal status. Emitted atomically with releasing the
    /// lock, on **every** terminal path — completed, failed, incomplete,
    /// cancelled and reaped alike. A path that forgets this leaves the session
    /// locked forever, which is why it is asserted at the port contract level.
    #[serde(rename = "session.turn_completed")]
    TurnCompleted {
        response_id: ResponseId,
        status: ResponseStatus,
    },

    /// A response record was deleted (`DELETE /v1/responses/{id}`), so every
    /// device can drop the corresponding bubble.
    ///
    /// Deletion stays **record-level** (D24): descendants keep resolving with
    /// the history they inherited, including this response's items. The event
    /// therefore describes a change to what is *listable*, not a rewrite of
    /// what any already-issued response can see.
    #[serde(rename = "session.response_deleted")]
    ResponseDeleted { response_id: ResponseId },

    /// Business-side event, ordered in the same sequence space as the
    /// conversation events above.
    ///
    /// This is the **only** open variant in the system. The envelope is closed
    /// (`kind` and `payload`, nothing else); the payload is opaque and bounded
    /// by size and JSON depth but not by schema — the service cannot know the
    /// caller's domain. The compatibility layer keeps its strict
    /// closed-enum-plus-`deny_unknown_fields` posture untouched (D22 ②).
    ///
    /// Business events **never enter model context**: context assembly reads the
    /// response chain and never the event stream.
    #[serde(rename = "session.business")]
    Business { kind: String, payload: Value },
}

impl SessionEventKind {
    /// The wire tag, also used as the discriminator in logs and metrics.
    ///
    /// Paired with the `serde(rename)` attributes above and kept honest by
    /// `every_variant_tags_itself_and_round_trips`, matching how
    /// [`crate::events::ResponseEventKind`] handles the same problem.
    ///
    /// Business events report the variant name only — the caller-supplied `kind`
    /// is data, and folding data into a metric label would make the label set
    /// unbounded.
    pub fn as_str(&self) -> &'static str {
        match self {
            SessionEventKind::SessionCreated => "session.created",
            SessionEventKind::TurnStarted { .. } => "session.turn_started",
            SessionEventKind::TurnCompleted { .. } => "session.turn_completed",
            SessionEventKind::ResponseDeleted { .. } => "session.response_deleted",
            SessionEventKind::Business { .. } => "session.business",
        }
    }

    /// The response this event refers to, if any. There is no content here by
    /// construction — only this reference.
    pub fn response_id(&self) -> Option<&ResponseId> {
        match self {
            SessionEventKind::TurnStarted { response_id }
            | SessionEventKind::TurnCompleted { response_id, .. }
            | SessionEventKind::ResponseDeleted { response_id } => Some(response_id),
            SessionEventKind::SessionCreated | SessionEventKind::Business { .. } => None,
        }
    }
}

/// One entry in a session's event stream.
///
/// `seq` is 0-based and contiguous per session (INV-11), matching the
/// per-response event log so both streams are read with one cursor rule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionEvent {
    pub session_id: SessionId,
    pub seq: u64,
    pub kind: SessionEventKind,
    pub ts_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resp() -> ResponseId {
        ResponseId::new(crate::ids::NodeTag::parse("n1").unwrap())
    }

    #[test]
    fn lock_state_reports_its_holder() {
        assert!(!LockState::Idle.is_busy());
        assert_eq!(LockState::Idle.holder(), None);

        let id = resp();
        let busy = LockState::Busy {
            response_id: id.clone(),
        };
        assert!(busy.is_busy());
        assert_eq!(busy.holder(), Some(&id));
    }

    #[test]
    fn a_new_session_starts_idle() {
        let s = Session::new(
            SessionId::new(),
            TenantId::parse("t1").unwrap(),
            ConversationId::new(),
            42,
        );
        assert_eq!(s.lock_state, LockState::Idle);
        assert_eq!(s.created_at_ms, 42);
    }

    #[test]
    fn events_carry_references_never_content() {
        // The single-source-of-truth rule is structural: no variant has a field
        // able to hold conversation text. Serialising every variant and
        // checking the payload keys is the cheapest way to keep it that way —
        // adding an items field to any variant fails here.
        let id = resp();
        for kind in [
            SessionEventKind::SessionCreated,
            SessionEventKind::TurnStarted {
                response_id: id.clone(),
            },
            SessionEventKind::TurnCompleted {
                response_id: id.clone(),
                status: ResponseStatus::Completed,
            },
            SessionEventKind::ResponseDeleted {
                response_id: id.clone(),
            },
        ] {
            let value = serde_json::to_value(&kind).unwrap();
            let object = value.as_object().unwrap();
            for key in object.keys() {
                assert!(
                    matches!(key.as_str(), "type" | "response_id" | "status"),
                    "`{key}` on {kind:?} may carry conversation content; \
                     session events must reference it instead"
                );
            }
        }
    }

    #[test]
    fn business_is_the_only_open_variant() {
        let business = SessionEventKind::Business {
            kind: "file_uploaded".into(),
            payload: serde_json::json!({ "path": "/tmp/a" }),
        };
        assert_eq!(business.as_str(), "session.business");
        assert_eq!(business.response_id(), None);

        // Its label never includes the caller-supplied kind, which would make
        // the metric label set unbounded.
        assert!(!business.as_str().contains("file_uploaded"));
    }

    #[test]
    fn every_variant_tags_itself_and_round_trips() {
        let id = resp();
        for kind in [
            SessionEventKind::SessionCreated,
            SessionEventKind::TurnStarted {
                response_id: id.clone(),
            },
            SessionEventKind::TurnCompleted {
                response_id: id.clone(),
                status: ResponseStatus::Cancelled,
            },
            SessionEventKind::ResponseDeleted {
                response_id: id.clone(),
            },
            SessionEventKind::Business {
                kind: "k".into(),
                payload: Value::Null,
            },
        ] {
            let json = serde_json::to_value(&kind).unwrap();
            assert_eq!(
                json.get("type").and_then(Value::as_str),
                Some(kind.as_str()),
                "as_str() must match the wire tag for {kind:?}"
            );
            assert!(
                kind.as_str().starts_with("session."),
                "self-hosted events must stay inside the `session.` namespace, \
                 clear of upstream's: {kind:?}"
            );
            assert_eq!(
                serde_json::from_value::<SessionEventKind>(json).unwrap(),
                kind
            );
        }
    }

    #[test]
    fn unknown_event_types_are_rejected() {
        // The envelope stays closed even though the payload is open (D22 ②).
        let err = serde_json::from_value::<SessionEventKind>(serde_json::json!({
            "type": "turn.exploded",
            "response_id": resp().to_string(),
        }));
        assert!(err.is_err(), "unknown event types must not deserialize");
    }
}
