//! Two-layer matcher (design/01 §1). Does not embed sandbox interpreter.

use nova_core::{Priority, TaskSpec, WorkerProfile};
use nova_ports::{FilterExpr, PolicySandbox, SandboxInput};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatcherVersion(pub String);

impl Default for MatcherVersion {
    fn default() -> Self {
        Self("v0".into())
    }
}

pub struct Matcher {
    version: MatcherVersion,
}

impl Matcher {
    pub fn new(version: impl Into<String>) -> Self {
        Self {
            version: MatcherVersion(version.into()),
        }
    }

    pub fn version(&self) -> &MatcherVersion {
        &self.version
    }

    /// Coarse filter — over-approximation (invariant S). Default: pending only.
    pub fn coarse_filter(&self, _worker: &WorkerProfile, limit: usize) -> FilterExpr {
        FilterExpr {
            pending_only: true,
            limit,
        }
    }

    /// Rank: lower is examined first. Includes aging term (D5).
    pub fn rank(&self, task: &TaskSpec, now_ms: u64) -> i64 {
        let age = now_ms.saturating_sub(task.submitted_at_ms) as i64;
        let prio = match task.priority {
            Priority::High => 0,
            Priority::Normal => 1_000_000,
            Priority::Low => 2_000_000,
        };
        prio - age.min(500_000)
    }

    pub async fn eligible(
        &self,
        sandbox: &dyn PolicySandbox,
        task: &TaskSpec,
        worker: &WorkerProfile,
    ) -> bool {
        if worker.remaining_capacity < task.capacity.units {
            return false;
        }
        let input = SandboxInput {
            task: task.clone(),
            worker: worker.clone(),
        };
        match sandbox.eval(&task.predicate, &input, 64).await {
            Ok(v) => v,
            Err(_) => false,
        }
    }

    pub fn feasible_at_full(&self, task: &TaskSpec, worker_total: u32) -> bool {
        task.capacity.units <= worker_total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nova_core::{CapacityNeed, TaskId, TaskKind, WorkerId};

    #[test]
    fn high_priority_ranks_first() {
        let m = Matcher::new("t");
        let high = TaskSpec {
            id: TaskId::new(),
            kind: TaskKind::Agent,
            priority: Priority::High,
            capacity: CapacityNeed { units: 1 },
            submitted_at_ms: 0,
            predicate: String::new(),
        };
        let low = TaskSpec {
            id: TaskId::new(),
            kind: TaskKind::Agent,
            priority: Priority::Low,
            capacity: CapacityNeed { units: 1 },
            submitted_at_ms: 0,
            predicate: String::new(),
        };
        assert!(m.rank(&high, 0) < m.rank(&low, 0));
        let _ = WorkerId::new();
    }
}
