use async_trait::async_trait;
use nova_core::{Attempt, TaskId, TaskSpec, TaskState, WorkerId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// INV-20: capacity re-checked atomically at claim time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapacityAssertion {
    pub required: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimOutcome {
    Claimed { attempt: Attempt },
    Conflict,
    NotFound,
    CapacityRejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterExpr {
    /// Closed operator set — adapters compile to parameterized queries.
    pub pending_only: bool,
    pub limit: usize,
}

#[derive(Debug, Clone)]
pub struct TaskRecord {
    pub spec: TaskSpec,
    pub state: TaskState,
    pub attempt: Attempt,
    pub owner: Option<WorkerId>,
    pub exec_deadline_ms: Option<u64>,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("internal: {0}")]
    Internal(String),
}

/// INV-1: the only claim entry — no get+update combo.
#[async_trait]
pub trait TaskStore: Send + Sync {
    async fn insert(&self, spec: TaskSpec) -> Result<(), StoreError>;

    async fn try_claim(
        &self,
        task_id: &TaskId,
        expected_state: TaskState,
        expected_attempt: Attempt,
        worker: &WorkerId,
        capacity_assert: &CapacityAssertion,
        exec_deadline_ms: u64,
    ) -> Result<ClaimOutcome, StoreError>;

    async fn list_candidates(&self, filter: &FilterExpr) -> Result<Vec<TaskSpec>, StoreError>;

    async fn get(&self, task_id: &TaskId) -> Result<Option<TaskRecord>, StoreError>;

    async fn finish(
        &self,
        task_id: &TaskId,
        expected_attempt: Attempt,
        to: TaskState,
    ) -> Result<bool, StoreError>;

    async fn release_to_pending(
        &self,
        task_id: &TaskId,
        expected_attempt: Attempt,
    ) -> Result<bool, StoreError>;

    async fn list_all(&self, limit: usize) -> Result<Vec<TaskRecord>, StoreError>;
}
