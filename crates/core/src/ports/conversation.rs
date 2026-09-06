use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::conversation::Conversation;
use crate::ids::{ConversationId, ResponseId, TenantId};

#[derive(Debug, Error, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConversationError {
    /// The conversation does not exist, or belongs to another tenant. One error
    /// for both so ids cannot be probed (SEC-2).
    #[error("not found")]
    NotFound,
    #[error("capacity exceeded")]
    CapacityExceeded,
    /// Store is unreachable. Callers must **reject the write** rather than
    /// proceed without persisting (INV-46).
    #[error("unavailable")]
    Unavailable,
    #[error("read only")]
    ReadOnly,
    #[error("internal: {0}")]
    Internal(String),
}

/// Storage for the upstream-compatible conversation pointer.
///
/// Six methods, and none of them touches an item. That is the whole point: the
/// conversation records *where* the chain ends, and the chain itself is already
/// persisted by [`crate::ports::ContextStore`]. There is no `snapshot_items`
/// here, and no hot read path to keep indexed, because assembling context
/// remains a single `resolve_chain` call against the materialised snapshot
/// (D24) — unchanged by the presence of conversations.
#[async_trait]
pub trait ConversationStore: Send + Sync {
    async fn create(
        &self,
        conversation: Conversation,
    ) -> Result<Conversation, ConversationError>;

    async fn get(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<Option<Conversation>, ConversationError>;

    /// Replace `metadata` wholesale and return the updated record.
    ///
    /// Wholesale rather than merge-patch because upstream models this as a POST
    /// of the new value; a merge would need a way to express deletion, which the
    /// wire format does not have.
    async fn update_metadata(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        metadata: BTreeMap<String, String>,
    ) -> Result<Conversation, ConversationError>;

    /// Delete a conversation. Returns whether one was removed.
    ///
    /// Response records are **not** cascaded, matching upstream ("Items in the
    /// conversation will not be deleted") and matching the record-level deletion
    /// rule already in force for chains (D24). Removing the pointer ends the
    /// conversation; it does not erase what was said.
    async fn delete(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<bool, ConversationError>;

    async fn delete_by_tenant(&self, tenant: &TenantId) -> Result<u64, ConversationError>;

    /// Move the tail pointer to `last`, after a turn reaches a terminal status.
    ///
    /// **Last write wins.** Two turns racing on the same conversation leave
    /// whichever finished last as the tail; the other's chain survives intact
    /// and addressable, it simply is not the tail any more. This is a deliberate
    /// choice, not an oversight: a compare-and-set here would reject the loser
    /// with a conflict status that upstream never returns, breaking callers that
    /// drive the conversation with an official SDK and no session. Sessions that
    /// need serialised turns get that from the session lock
    /// ([`crate::ports::SessionStore::begin_turn`]), which refuses the second
    /// turn up front instead of letting both run and discarding one result.
    async fn advance(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        last: &ResponseId,
    ) -> Result<(), ConversationError>;

    /// Liveness probe backing the refuse-writes degrade (INV-46).
    async fn health(&self) -> Result<(), ConversationError>;
}
