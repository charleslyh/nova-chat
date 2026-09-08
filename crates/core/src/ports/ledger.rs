use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::context::{ResponseStatus, StoredResponse, Usage};
use crate::ids::{AgentId, Attempt, ConversationId, IdempotencyKey, ResponseId, TenantId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CreateOutcome {
    Accepted { response_id: ResponseId },
    Duplicate { response_id: ResponseId },
    /// INV-32: read-only degrade rejects new writes.
    ReadOnly,
    /// FR-33: queued/in-flight count at or above the configured limit.
    Overloaded,
    // Still no `Busy` here. The turn lock (D28) lives in `ConversationStore`,
    // not the ledger: a lock outcome belongs with the store that holds the lock,
    // and admission is refused before this port is reached.
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClaimedResponse {
    pub record: StoredResponse,
    pub attempt: Attempt,
    pub exec_deadline_ms: u64,
}

/// A claim the reap path took away from a holder that stopped reporting.
///
/// Carries the tenant and the conversation association, not just the id, because
/// reaping is a terminal transition and terminal transitions owe the conversation
/// a marker release (D28) — which needs a tenant. Reading them back with a
/// follow-up `get` would be a second read of a row the reaping statement already
/// had in hand, and one that could be deleted in between.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbortedClaim {
    pub response_id: ResponseId,
    pub previous_attempt: Attempt,
    pub tenant_id: TenantId,
    /// Conversation whose in-flight marker this claim held, if any. Reap **must**
    /// release it: the previous holder is gone and will never reach its own
    /// terminal path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation_id: Option<ConversationId>,
}

#[derive(Debug, Error, PartialEq, Eq, Serialize, Deserialize)]
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

    /// Take the next queued response for execution.
    ///
    /// Single-point conditional update (INV-1): the check and the transition to
    /// claimed happen in one atomic operation, so two concurrent callers cannot
    /// both succeed on the same response.
    ///
    /// Global claim (D25): any execution process may claim any queued response.
    /// The in-flight buffer is shared, so the producer is no longer tied to the
    /// creating node. The attempt fence still protects against double-claim and
    /// stale writes (INV-5/6).
    async fn claim(
        &self,
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
