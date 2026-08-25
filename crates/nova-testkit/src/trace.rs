use nova_core::{Attempt, IdempotencyKey, TaskId, WorkerId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TraceEvent {
    Submitted {
        task: TaskId,
        key: IdempotencyKey,
        at_ms: u64,
    },
    ClaimAttempted {
        task: TaskId,
        worker: WorkerId,
        ok: bool,
    },
    Claimed {
        task: TaskId,
        worker: WorkerId,
        attempt: Attempt,
        at_ms: u64,
    },
    CapacityChanged {
        worker: WorkerId,
        delta: i64,
        at_ms: u64,
    },
    Output {
        task: TaskId,
        attempt: Attempt,
        seq: u64,
        bytes: usize,
    },
    StateChanged {
        task: TaskId,
        from: String,
        to: String,
        at_ms: u64,
    },
    Rejected {
        reason: String,
        at_ms: u64,
    },
    FaultInjected {
        kind: String,
        target: String,
        at_ms: u64,
    },
    Terminal {
        task: TaskId,
        state: String,
        at_ms: u64,
    },
}

#[derive(Debug, Default, Clone)]
pub struct Trace {
    pub events: Vec<TraceEvent>,
}

impl Trace {
    pub fn push(&mut self, e: TraceEvent) {
        self.events.push(e);
    }
}
