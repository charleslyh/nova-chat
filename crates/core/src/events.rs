use serde::{Deserialize, Serialize};

use crate::ids::{Attempt, SessionId, TurnId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    TurnBegin,
    SessionBusy,
    SessionIdle,
    AttemptStarted,
    TextDelta,
    TurnDone,
    TurnFailed,
    AttemptAborted,
}

impl EventKind {
    /// INV-16: coalescible events may drop intermediate states.
    pub fn coalescible(self) -> bool {
        matches!(self, EventKind::TextDelta)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamEvent {
    pub session_id: SessionId,
    pub seq: u64,
    pub kind: EventKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<TurnId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<Attempt>,
    /// UTF-8 text or opaque payload (e.g. user message, delta text).
    #[serde(default)]
    pub payload: String,
}
