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
