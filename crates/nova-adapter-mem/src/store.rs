use std::collections::HashMap;

use async_trait::async_trait;
use nova_core::{Attempt, TaskId, TaskSpec, TaskState, WorkerId};
use nova_ports::{
    CapacityAssertion, ClaimOutcome, FilterExpr, StoreError, TaskRecord, TaskStore,
};
use parking_lot::Mutex;

#[derive(Default)]
struct Inner {
    tasks: HashMap<TaskId, TaskRecord>,
}

pub struct MemTaskStore {
    inner: Mutex<Inner>,
}

impl MemTaskStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
        }
    }
}

impl Default for MemTaskStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TaskStore for MemTaskStore {
    async fn insert(&self, spec: TaskSpec) -> Result<(), StoreError> {
        let mut g = self.inner.lock();
        if g.tasks.contains_key(&spec.id) {
            return Ok(());
        }
        g.tasks.insert(
            spec.id,
            TaskRecord {
                spec,
                state: TaskState::Pending,
                attempt: Attempt(0),
                owner: None,
                exec_deadline_ms: None,
            },
        );
        Ok(())
    }

    async fn try_claim(
        &self,
        task_id: &TaskId,
        expected_state: TaskState,
        expected_attempt: Attempt,
        worker: &WorkerId,
        capacity_assert: &CapacityAssertion,
        exec_deadline_ms: u64,
    ) -> Result<ClaimOutcome, StoreError> {
        let mut g = self.inner.lock();
        let Some(rec) = g.tasks.get_mut(task_id) else {
            return Ok(ClaimOutcome::NotFound);
        };
        if rec.state != expected_state || rec.attempt != expected_attempt {
            return Ok(ClaimOutcome::Conflict);
        }
        // INV-20: capacity asserted at claim; ledger checked by caller / claim layer.
        // Store only re-validates required units are non-zero placeholder consistency.
        if capacity_assert.required == 0 {
            return Ok(ClaimOutcome::CapacityRejected);
        }
        let new_attempt = rec.attempt.next();
        rec.state = TaskState::Claimed;
        rec.attempt = new_attempt;
        rec.owner = Some(*worker);
        rec.exec_deadline_ms = Some(exec_deadline_ms);
        Ok(ClaimOutcome::Claimed {
            attempt: new_attempt,
        })
    }

    async fn list_candidates(&self, filter: &FilterExpr) -> Result<Vec<TaskSpec>, StoreError> {
        let g = self.inner.lock();
        let mut out: Vec<_> = g
            .tasks
            .values()
            .filter(|r| !filter.pending_only || r.state == TaskState::Pending)
            .map(|r| r.spec.clone())
            .collect();
        out.truncate(filter.limit);
        Ok(out)
    }

    async fn get(&self, task_id: &TaskId) -> Result<Option<TaskRecord>, StoreError> {
        Ok(self.inner.lock().tasks.get(task_id).cloned())
    }

    async fn finish(
        &self,
        task_id: &TaskId,
        expected_attempt: Attempt,
        to: TaskState,
    ) -> Result<bool, StoreError> {
        let mut g = self.inner.lock();
        let Some(rec) = g.tasks.get_mut(task_id) else {
            return Ok(false);
        };
        if rec.attempt != expected_attempt {
            return Ok(false);
        }
        rec.state = to;
        // Keep owner for audit / sim timeline even after terminal.
        Ok(true)
    }

    async fn release_to_pending(
        &self,
        task_id: &TaskId,
        expected_attempt: Attempt,
    ) -> Result<bool, StoreError> {
        let mut g = self.inner.lock();
        let Some(rec) = g.tasks.get_mut(task_id) else {
            return Ok(false);
        };
        if rec.attempt != expected_attempt {
            return Ok(false);
        }
        rec.state = TaskState::Pending;
        rec.owner = None;
        rec.exec_deadline_ms = None;
        Ok(true)
    }

    async fn list_all(&self, limit: usize) -> Result<Vec<TaskRecord>, StoreError> {
        let g = self.inner.lock();
        Ok(g.tasks.values().take(limit).cloned().collect())
    }
}

// Shared store handle type alias helper removed (unused).
