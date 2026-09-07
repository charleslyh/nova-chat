use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::context::ResponseStatus;
use crate::conversation::{Conversation, ConversationEvent, ConversationEventKind};
use crate::ids::{ConversationId, ResponseId, TenantId};

#[derive(Debug, Error, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConversationError {
    /// The conversation does not exist, or belongs to another tenant. One error
    /// for both so ids cannot be probed (SEC-2).
    #[error("not found")]
    NotFound,
    /// A turn is already in flight for this conversation (D28). The holder is
    /// reported so a caller can decide whether to take the stale lock over — the
    /// same recoverability contract the old session lock had (D26).
    #[error("conversation is busy with {holder}")]
    Busy { holder: ResponseId },
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
    /// drive the conversation with an official SDK and no session. Turns that
    /// need serialisation get it from [`ConversationStore::acquire_active`],
    /// which refuses the second turn up front instead of letting both run and
    /// discarding one result.
    async fn advance(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        last: &ResponseId,
    ) -> Result<(), ConversationError>;

    /// Occupy the in-flight marker and emit `TurnStarted`, atomically (D28).
    ///
    /// The marker transition and the event that announces it must land together
    /// or not at all — a crash between two separate calls leaves either a busy
    /// conversation with nothing on the stream to explain it, or an announced
    /// turn no marker is holding. Atomicity is a property of the store, so the
    /// pairing lives here.
    ///
    /// Contract:
    /// - `active` empty → set it to `response_id`, emit `TurnStarted`, return its seq
    /// - `active` already holds another response → [`ConversationError::Busy`]
    ///   naming the holder, and **write nothing** (no event, no partial state)
    /// - re-entering with the id that already holds the marker succeeds without a
    ///   second event, so an execution-side retry is harmless
    async fn acquire_active(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        response_id: &ResponseId,
        now_ms: u64,
    ) -> Result<u64, ConversationError>;

    /// Clear the in-flight marker and emit `TurnCompleted`, atomically (D28).
    ///
    /// Must be called on **every** terminal path. Conditional on `response_id`
    /// still being the holder, and idempotent for the same id.
    async fn release_active(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        response_id: &ResponseId,
        status: ResponseStatus,
        now_ms: u64,
    ) -> Result<u64, ConversationError>;

    /// Release a marker held by `holder` without a `TurnCompleted`, for one
    /// situation only: the holder is already terminal but its marker was never
    /// released. No event is emitted — the terminal event was already emitted by
    /// whoever completed the response.
    ///
    /// Returns whether a marker was actually released. Conditional on `holder`
    /// still being the holder.
    async fn release_stale_active(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        holder: &ResponseId,
    ) -> Result<bool, ConversationError>;

    /// Append a non-turn event (`Business` or `ResponseDeleted`) and return its
    /// sequence number. Turn boundaries go through `acquire_active` /
    /// `release_active` instead, since those must be paired with the marker.
    async fn append_event(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        kind: ConversationEventKind,
        now_ms: u64,
    ) -> Result<u64, ConversationError>;

    /// Read events strictly after `starting_after`. `None` means "from the
    /// beginning". `wait_ms` allows a long poll for live continuation.
    async fn read_after(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        starting_after: Option<u64>,
        limit: usize,
        wait_ms: u64,
    ) -> Result<Vec<ConversationEvent>, ConversationError>;

    /// Every conversation the tenant owns, newest first (for list rendering).
    async fn list(&self, tenant: &TenantId) -> Result<Vec<Conversation>, ConversationError>;

    /// Set the per-conversation event-stream bound. Reaching it refuses the
    /// append rather than evicting the oldest events.
    fn set_max_events_per_conversation(&self, limit: usize);

    /// Liveness probe backing the refuse-writes degrade (INV-46).
    async fn health(&self) -> Result<(), ConversationError>;
}
