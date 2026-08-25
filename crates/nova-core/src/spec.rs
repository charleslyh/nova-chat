use serde::{Deserialize, Serialize};

use crate::ids::TaskId;

/// Business task kind — data only; must not branch core flows (D16).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    AigcImage,
    AigcVideo,
    Agent,
}

impl TaskKind {
    /// Per-type execution deadline override (seconds). Default ceiling 4h.
    pub fn exec_deadline_secs(self) -> u64 {
        match self {
            TaskKind::AigcImage => 30 * 60,
            TaskKind::AigcVideo => 4 * 60 * 60,
            TaskKind::Agent => 60 * 60,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    Low = 0,
    Normal = 1,
    High = 2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapacityNeed {
    pub units: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSpec {
    pub id: TaskId,
    pub kind: TaskKind,
    pub priority: Priority,
    pub capacity: CapacityNeed,
    /// Age hint for ranking (logical ms).
    pub submitted_at_ms: u64,
    /// Optional DSL predicate program (opaque to core).
    pub predicate: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerProfile {
    pub id: crate::ids::WorkerId,
    pub total_capacity: u32,
    pub remaining_capacity: u32,
    pub labels: Vec<String>,
}
