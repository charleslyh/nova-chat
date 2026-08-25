use nova_core::{
    CapacityNeed, EventKind, OutputEvent, Priority, TaskId, TaskKind, TaskSpec, WorkerId,
    WorkerProfile,
};
use nova_ports::StreamChannel;
use std::sync::Arc;

/// In-process mock worker for L1.
pub struct MockWorker {
    pub profile: WorkerProfile,
    pub kind_bias: TaskKind,
}

impl MockWorker {
    pub fn new(total: u32, kind_bias: TaskKind) -> Self {
        Self {
            profile: WorkerProfile {
                id: WorkerId::new(),
                total_capacity: total,
                remaining_capacity: total,
                labels: vec![],
            },
            kind_bias,
        }
    }

    pub async fn emit_progress(
        &self,
        stream: Arc<dyn StreamChannel>,
        task: &TaskSpec,
        attempt: nova_core::Attempt,
    ) {
        let rate = match task.kind {
            TaskKind::Agent => 3,
            TaskKind::AigcImage => 1,
            TaskKind::AigcVideo => 1,
        };
        for i in 0..rate {
            let _ = stream
                .append(OutputEvent {
                    task_id: task.id,
                    attempt,
                    seq: 0,
                    kind: if matches!(task.kind, TaskKind::Agent) {
                        EventKind::TextDelta
                    } else {
                        EventKind::Progress
                    },
                    payload: format!("p{i}").into_bytes(),
                })
                .await;
        }
    }
}

pub fn sample_task(kind: TaskKind, priority: Priority, units: u32, at_ms: u64) -> TaskSpec {
    TaskSpec {
        id: TaskId::new(),
        kind,
        priority,
        capacity: CapacityNeed { units },
        submitted_at_ms: at_ms,
        predicate: String::new(),
    }
}
