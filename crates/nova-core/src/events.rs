use serde::{Deserialize, Serialize};

use crate::ids::{Attempt, TaskId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Progress,
    TextDelta,
    Artifact,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputEvent {
    pub task_id: TaskId,
    pub attempt: Attempt,
    pub seq: u64,
    pub kind: EventKind,
    pub payload: Vec<u8>,
}
