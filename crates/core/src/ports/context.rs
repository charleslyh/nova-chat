use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::context::{ChainLimits, ResolvedContext, ResponseStatus, StoredResponse, Usage};
use crate::ids::{ResponseId, TenantId};
use crate::protocol::ResponseItem;

#[derive(Debug, Error, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContextError {
    #[error("not found")]
    NotFound,
    /// Referenced link exists but was created with `store: false`, so it holds
    /// no items and cannot be chained (FR-18).
    #[error("referenced response was not stored")]
    NotStored,
    /// The addressed anchor does not exist or belongs to another tenant.
    ///
    /// With materialised history (D24) there is no walk, so a *missing link* can
    /// never be encountered — only a missing *anchor*. Reported identically for
    /// "absent" and "foreign" so ids cannot be probed (SEC-2).
    #[error("chain broken at {0}")]
    ChainBroken(String),
    #[error("chain exceeds depth limit {limit}")]
    ChainTooLong { limit: usize },
    #[error("chain exceeds byte limit {limit}")]
    ChainTooLarge { limit: usize },
    /// A link belongs to another tenant. Detected per hop (INV-42) and always
    /// fatal — never skipped.
    #[error("chain crosses tenant boundary")]
    CrossTenant,
    #[error("integrity mismatch")]
    IntegrityMismatch,
    #[error("capacity exceeded")]
    CapacityExceeded,
    /// Store is unreachable. Callers must **reject the write** rather than
    /// proceed without storing (INV-46).
    #[error("unavailable")]
    Unavailable,
    #[error("read only")]
    ReadOnly,
    #[error("internal: {0}")]
    Internal(String),
}

/// Persistence for response items plus chain resolution.
///
/// Named `ContextStore` rather than `ConversationStore` on purpose: the latter
/// reads as "the thing the UI renders", which is exactly what this does *not*
/// hold. It holds the items that make up model context.
#[async_trait]
pub trait ContextStore: Send + Sync {
    async fn put(&self, record: StoredResponse) -> Result<(), ContextError>;

    /// Commit the final output.
    ///
    /// Items are supplied by the execution side directly; they are **never**
    /// derived by replaying the event stream (INV-48).
    async fn append_output(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
        items: Vec<ResponseItem>,
        usage: Usage,
        status: ResponseStatus,
        now_ms: u64,
    ) -> Result<(), ContextError>;

    async fn get(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
    ) -> Result<Option<StoredResponse>, ContextError>;

    /// Return the context visible to `from`, in chronological order.
    ///
    /// History is **materialised** (D24): `from` carries a flat copy of every
    /// ancestor's items, so this reads `from.context` plus `from`'s own items
    /// instead of walking `previous_response_id`. A downstream response therefore
    /// never depends on its ancestors still existing.
    ///
    /// Contract:
    /// - tenant is verified (INV-42)
    /// - `store == false` is rejected, not skipped
    /// - exceeding depth, item count or bytes is an error, never a truncation
    ///   (INV-41)
    /// - the result contains **only items**; no link's `instructions` are ever
    ///   included (INV-49)
    async fn resolve_chain(
        &self,
        tenant: &TenantId,
        from: &ResponseId,
        limits: ChainLimits,
    ) -> Result<ResolvedContext, ContextError>;

    /// Delete one response's record.
    ///
    /// Returns whether a record was removed.
    ///
    /// Deletion is **record-level** (D24): the response's own record is removed,
    /// and nothing else changes. Downstream responses survive and keep resolving,
    /// with the full history they inherited — including this response's content —
    /// because that history was copied into their snapshot at create time.
    /// "Remove from the conversation" removes the record, not the inherited copy.
    async fn delete(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
    ) -> Result<bool, ContextError>;

    async fn delete_by_tenant(&self, tenant: &TenantId) -> Result<u64, ContextError>;

    /// Remove records past their retention deadline, bounded per call.
    async fn sweep_expired(&self, now_ms: u64, limit: usize) -> Result<u64, ContextError>;

    /// Liveness probe backing the refuse-writes degrade (INV-46).
    async fn health(&self) -> Result<(), ContextError>;
}
