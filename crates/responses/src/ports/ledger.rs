use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::conversation::ConversationId;
use crate::identity::{AgentId, Attempt, IdempotencyKey, TenantId};
use crate::provenance::RequestProvenance;
use crate::response::{ResponseId, ResponseRecord, ResponseStatus};
use crate::usage::Usage;

use super::admission::AdmissionControl;
use super::store_error::StoreError;

/// What happened to a create.
///
/// `ReadOnly` and `Overloaded` are **not** errors: they are the ledger's normal
/// refusals, and the ingress layer turns them into 503/429. `Accepted` and
/// `Duplicate` both carry the record, so the caller never has to read back a row
/// the ledger already had in hand — and so this one type can serve as the whole
/// answer, instead of being re-wrapped in a near-identical capability-layer enum.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CreateOutcome {
    /// Persisted in `Queued` state.
    Accepted(Box<ResponseRecord>),
    /// Idempotent replay: the original generation, never a second one (FR-3).
    Duplicate(Box<ResponseRecord>),
    /// INV-32: read-only degrade rejects new writes.
    ReadOnly,
    /// FR-33: queued/in-flight count at or above the configured limit.
    Overloaded,
    // Still no `Busy` here. The turn lock (D28) lives with the conversation store,
    // not the ledger: a lock outcome belongs with the store that holds the lock,
    // and admission is refused before this port is reached.
}

impl CreateOutcome {
    /// The record behind either success, so a caller can render both the same way.
    pub fn record(&self) -> Option<&ResponseRecord> {
        match self {
            CreateOutcome::Accepted(record) | CreateOutcome::Duplicate(record) => Some(record),
            CreateOutcome::ReadOnly | CreateOutcome::Overloaded => None,
        }
    }

    pub fn accepted(record: ResponseRecord) -> Self {
        CreateOutcome::Accepted(Box::new(record))
    }

    pub fn duplicate(record: ResponseRecord) -> Self {
        CreateOutcome::Duplicate(Box::new(record))
    }
}

/// A response taken for execution.
///
/// The attempt is **not** repeated here: it is on `record.attempt`, which the claim
/// itself just raised. Two copies of a fencing token is one copy too many — the
/// runner reads the fence through [`Self::provenance`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClaimedResponse {
    /// Ledger metadata for the claimed response. Carries **no** history: the
    /// snapshot to run against is read from the conversation store via the record's
    /// anchor (D30).
    pub record: ResponseRecord,
    /// Wall-clock milliseconds after which this attempt is forfeit.
    pub exec_deadline_ms: u64,
}

impl ClaimedResponse {
    /// Traceability for this attempt, derived rather than assembled by the caller.
    pub fn provenance(&self) -> RequestProvenance {
        RequestProvenance {
            response_id: self.record.response_id.clone(),
            attempt: self.record.attempt,
            exec_deadline_ms: self.exec_deadline_ms,
        }
    }
}

/// A claim the reap path took away from a holder that stopped reporting.
///
/// Carries the tenant and the conversation association, not just the id, because
/// reaping is a terminal transition and terminal transitions owe the conversation a
/// marker release (D28) — which needs a tenant. Reading them back with a follow-up
/// `get` would be a second read of a row the reaping statement already had in hand,
/// and one that could be deleted in between.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbortedClaim {
    pub response_id: ResponseId,
    pub previous_attempt: Attempt,
    pub tenant_id: TenantId,
    /// Conversation whose in-flight marker this claim held, if any. Reap **must**
    /// release it: the previous holder is gone and will never reach its own terminal
    /// path.
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
    /// Infrastructure failure (read-only / unreachable / internal).
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// Generation ledger: lifecycle, ownership, idempotency and usage.
#[async_trait]
pub trait ResponseLedger: AdmissionControl {
    /// Persist a new response in `Queued` state, carrying metadata only (D30): the
    /// record holds no snapshot, so there is no context write to pair this with at
    /// create time. The atomicity boundary moved to terminal time (`complete` +
    /// `ConversationSnapshots::append_turn`, INV-34).
    ///
    /// The record is returned in the outcome because the implementation may enrich
    /// it (the integrity tag is computed here), and the caller must see what was
    /// actually stored rather than what it proposed.
    async fn create(
        &self,
        record: ResponseRecord,
        idempotency_key: IdempotencyKey,
        now_ms: u64,
    ) -> Result<CreateOutcome, LedgerError>;

    /// Take the next queued response for execution.
    ///
    /// Single-point conditional update (INV-1): the check and the transition to
    /// claimed happen in one atomic operation, so two concurrent callers cannot both
    /// succeed on the same response.
    ///
    /// Global claim (D25): any execution process may claim any queued response. The
    /// in-flight buffer is shared, so the producer is no longer tied to the creating
    /// node. The attempt fence still protects against double-claim and stale writes
    /// (INV-5/6).
    async fn claim(
        &self,
        agent_id: AgentId,
        now_ms: u64,
        exec_ttl: Duration,
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

    /// Terminate on request (FR-7). Records partial usage of the running attempt so
    /// billing stays correct (INV-51).
    async fn cancel(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
        now_ms: u64,
    ) -> Result<(), LedgerError>;

    /// Reap timed-out claims, raising the attempt fence. Returns aborted claims so
    /// the caller can emit failure events and close their buffers.
    async fn reap(
        &self,
        now_ms: u64,
        heartbeat_ttl: Duration,
    ) -> Result<Vec<AbortedClaim>, LedgerError>;

    /// Book usage consumed by an attempt that was abandoned (INV-51).
    async fn record_partial_usage(
        &self,
        response_id: &ResponseId,
        attempt: Attempt,
        usage: Usage,
    ) -> Result<(), LedgerError>;

    async fn get(&self, response_id: &ResponseId) -> Result<Option<ResponseRecord>, LedgerError>;

    /// Remove a response's record (record-level delete, D30). Returns whether a
    /// record was removed. The conversation snapshot is **not** touched — the
    /// inherited copy lives on there, exactly as "remove from the conversation"
    /// requires.
    async fn delete(&self, response_id: &ResponseId) -> Result<bool, LedgerError>;

    /// Bulk-erase every response a tenant owns (FR-21).
    async fn delete_by_tenant(&self, tenant: &TenantId) -> Result<u64, LedgerError>;

    /// Append fence validation (INV-6).
    async fn check_attempt(
        &self,
        response_id: &ResponseId,
        attempt: Attempt,
    ) -> Result<(), LedgerError>;

    /// Count of non-terminal responses, for overload rejection (FR-33) and for
    /// draining during graceful shutdown (FR-34).
    async fn in_flight(&self) -> Result<usize, LedgerError>;
}
