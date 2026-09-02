use async_trait::async_trait;
use thiserror::Error;

use crate::context::{ChainLimits, ResolvedContext, ResponseStatus, StoredResponse, Usage};
use crate::ids::{ResponseId, TenantId};
use crate::protocol::ResponseItem;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ContextError {
    #[error("not found")]
    NotFound,
    /// Referenced link exists but was created with `store: false`, so it holds
    /// no items and cannot be chained (FR-18).
    #[error("referenced response was not stored")]
    NotStored,
    /// A link in the chain is missing or has expired.
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
    /// Whether the backing store is shared across nodes.
    ///
    /// This single flag decides how the ingress layer reaches content:
    /// `false` → forward to the owning node; `true` → connect directly.
    /// When it returns `true`, **chain affinity routing must be disabled**,
    /// otherwise long conversations pin all their traffic to one node and
    /// create a hotspot (D21).
    fn is_shared(&self) -> bool;

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

    /// Walk backwards from `from` and return history in chronological order.
    ///
    /// Contract:
    /// - tenant is verified on **every** hop (INV-42)
    /// - links with `stored == false` are rejected, not skipped
    /// - exceeding depth or bytes is an error, never a truncation (INV-41)
    /// - the result contains **only items**; no link's `instructions` are ever
    ///   included (INV-49)
    async fn resolve_chain(
        &self,
        tenant: &TenantId,
        from: &ResponseId,
        limits: ChainLimits,
    ) -> Result<ResolvedContext, ContextError>;

    /// Returns whether a record was removed.
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
