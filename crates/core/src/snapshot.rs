use serde::{Deserialize, Serialize};

use crate::ids::{SessionId, TurnId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bubble {
    pub role: String,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<TurnId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSnapshot {
    pub session_id: SessionId,
    pub snapshot_seq: u64,
    pub bubbles: Vec<Bubble>,
    /// In-flight turn ids (max_in_flight=1 → 0 or 1).
    pub running: Vec<TurnId>,
}

impl SessionSnapshot {
    /// Open-screen contract (FR-9 / INV-13): after `GET snapshot`, start SSE at this `from_seq`.
    ///
    /// Empty sessions use `snapshot_seq == 0` → stream from `1`. Callers must not rely on
    /// replaying the full hot log from seq 1 when a snapshot tip is available.
    pub fn stream_from_seq(&self) -> u64 {
        self.snapshot_seq.max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_screen_cursor() {
        let sid = SessionId::new();
        let empty = SessionSnapshot {
            session_id: sid,
            snapshot_seq: 0,
            bubbles: vec![],
            running: vec![],
        };
        assert_eq!(empty.stream_from_seq(), 1);
        let mid = SessionSnapshot {
            session_id: sid,
            snapshot_seq: 7,
            bubbles: vec![],
            running: vec![],
        };
        assert_eq!(mid.stream_from_seq(), 7);
    }
}
