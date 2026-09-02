use async_trait::async_trait;
use thiserror::Error;

use crate::context::{ResponseStatus, StoredResponse, Usage};
use crate::ids::{AgentId, Attempt, IdempotencyKey, NodeTag, ResponseId, TenantId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateOutcome {
    Accepted { response_id: ResponseId },
    Duplicate { response_id: ResponseId },
    /// INV-32: read-only degrade rejects new writes.
    ReadOnly,
    /// FR-33: queued/in-flight count at or above the configured limit.
    Overloaded,
    // No `Busy`: there is no session lock any more (D20 ①).
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimedResponse {
    pub record: StoredResponse,
    pub attempt: Attempt,
    pub exec_deadline_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbortedClaim {
    pub response_id: ResponseId,
    pub previous_attempt: Attempt,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LedgerError {
    #[error("not found")]
    NotFound,
    #[error("stale attempt")]
    StaleAttempt,
    #[error("invalid transition: {0}")]
    InvalidTransition(String),
    #[error("read only")]
    ReadOnly,
    #[error("unavailable")]
    Unavailable,
    #[error("internal: {0}")]
    Internal(String),
}

/// Generation ledger: lifecycle, ownership, idempotency and usage.
///
/// Shares storage and a transaction with the context store (D21 ①), so a
/// created response and its stored items can never disagree.
#[async_trait]
pub trait ResponseLedger: Send + Sync {
    /// Persist a new response in `Queued` state. Must be atomic with the
    /// context write when `record.stored` is true (INV-34).
    async fn create(
        &self,
        record: StoredResponse,
        idempotency_key: IdempotencyKey,
        now_ms: u64,
    ) -> Result<CreateOutcome, LedgerError>;

    /// Take the next queued response **belonging to `node`** for execution.
    ///
    /// Single-point conditional update (INV-1): the check and the transition to
    /// claimed happen in one atomic operation, so two concurrent callers cannot
    /// both succeed on the same response.
    ///
    /// # Why `node` is a parameter and not an implementation detail
    ///
    /// A response is executed by the node that created it (FR-4 / D23), because
    /// that node — and only that node — holds its in-flight event buffer. The
    /// buffer is a `VecDeque` in one process's heap by deliberate design (D21), so
    /// there is no shared endpoint another node could append to.
    ///
    /// Handing node-b's response to node-a therefore produces a response whose
    /// increments land in the wrong process: the subscriber, routing by the node
    /// tag inside the id, is sent to node-b and sees only `Created` — never any
    /// output, and never an error either. Silent, and indistinguishable from a
    /// model that simply produced nothing.
    ///
    /// This was a real defect once the ledger became shared: the per-node ledger
    /// of the in-memory backend had made the constraint hold automatically, so
    /// nothing expressed it. Implementations **must** filter by `node`.
    async fn claim(
        &self,
        node: &NodeTag,
        agent_id: AgentId,
        now_ms: u64,
        exec_ttl_ms: u64,
    ) -> Result<Option<ClaimedResponse>, LedgerError>;

    async fn heartbeat(&self, agent_id: AgentId, now_ms: u64) -> Result<(), LedgerError>;

    async fn complete(
        &self,
        response_id: &ResponseId,
        expected_attempt: Attempt,
        status: ResponseStatus,
        usage: Usage,
        now_ms: u64,
    ) -> Result<(), LedgerError>;

    /// Terminate on request (FR-7). Records partial usage of the running
    /// attempt so billing stays correct (INV-51).
    async fn cancel(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
        now_ms: u64,
    ) -> Result<(), LedgerError>;

    /// Reap timed-out claims, raising the attempt fence. Returns aborted claims
    /// so the caller can emit failure events and close their buffers.
    async fn reap(
        &self,
        now_ms: u64,
        heartbeat_ttl_ms: u64,
    ) -> Result<Vec<AbortedClaim>, LedgerError>;

    /// Startup orphan sweep (INV-45): everything still non-terminal that belongs
    /// to this node is failed immediately, because its in-flight buffer died
    /// with the previous process.
    async fn reclaim_orphans(
        &self,
        node_tag: &NodeTag,
        now_ms: u64,
    ) -> Result<Vec<AbortedClaim>, LedgerError>;

    /// Book usage consumed by an attempt that was abandoned (INV-51).
    async fn record_partial_usage(
        &self,
        response_id: &ResponseId,
        attempt: Attempt,
        usage: Usage,
    ) -> Result<(), LedgerError>;

    async fn get(&self, response_id: &ResponseId) -> Result<Option<StoredResponse>, LedgerError>;

    /// Append fence validation (INV-6).
    async fn check_attempt(
        &self,
        response_id: &ResponseId,
        attempt: Attempt,
    ) -> Result<(), LedgerError>;

    /// Count of non-terminal responses, for overload rejection (FR-33) and for
    /// draining during graceful shutdown (FR-34).
    async fn in_flight(&self) -> Result<usize, LedgerError>;

    // --- runtime controls ---
    //
    // Part of the port rather than of a concrete adapter, so the ingress layer
    // never needs to know which backend is mounted. Synchronous because they
    // only flip process-local state.

    /// INV-32: reject upstream writes while reads keep working.
    fn set_read_only(&self, enabled: bool);
    fn is_read_only(&self) -> bool;

    /// FR-33 overload threshold.
    fn set_pending_limit(&self, limit: usize);
    fn pending_limit(&self) -> usize;
}
