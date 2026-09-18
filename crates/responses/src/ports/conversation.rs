//! The conversation's storage, as four traits.
//!
//! One trait with eighteen methods obliged every backend to implement the whole
//! surface, and every consumer to depend on it: the SSE skeleton needs to read an
//! event stream and nothing else, yet it took a handle that could also delete the
//! conversation. The split is by *reason to change* — records, snapshot, turn lock,
//! event stream — and [`ConversationStore`] remains as the composition of all four,
//! so assembly still mounts one object.

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::context::ResolvedContext;
use crate::conversation::{
    Conversation, ConversationEvent, ConversationEventKind, ConversationId, TurnCommit,
};
use crate::identity::TenantId;
use crate::protocol::MetadataValue;
use crate::response::{ResponseId, ResponseStatus};

use super::store_error::StoreError;

#[derive(Debug, Error, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConversationError {
    /// The conversation does not exist, or belongs to another tenant. One error for
    /// both so ids cannot be probed (SEC-2).
    #[error("not found")]
    NotFound,
    /// A turn is already in flight for this conversation (D28). The holder is
    /// reported so a caller can decide whether to take the stale lock over.
    #[error("conversation is busy with {holder}")]
    Busy { holder: ResponseId },
    #[error("capacity exceeded")]
    CapacityExceeded,
    /// Infrastructure failure (read-only / unreachable / internal).
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// The conversation record itself: create, read, retire.
#[async_trait]
pub trait ConversationRepo: Send + Sync {
    async fn create(&self, conversation: Conversation) -> Result<Conversation, ConversationError>;

    async fn get(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<Option<Conversation>, ConversationError>;

    /// Replace `metadata` wholesale and return the updated record.
    ///
    /// Wholesale rather than merge-patch because upstream models this as a POST of
    /// the new value; a merge would need a way to express deletion, which the wire
    /// format does not have.
    async fn update_metadata(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        metadata: BTreeMap<String, MetadataValue>,
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

    /// Every conversation the tenant owns, newest first (for list rendering).
    async fn list(&self, tenant: &TenantId) -> Result<Vec<Conversation>, ConversationError>;

    /// Liveness probe backing the refuse-writes degrade (INV-46).
    async fn health(&self) -> Result<(), ConversationError>;
}

/// The conversation's materialised history — the system of record for content under
/// D30, replacing the removed per-response context store.
#[async_trait]
pub trait ConversationSnapshots: Send + Sync {
    /// Read the full history as it currently stands, oldest first. This is what an
    /// execution builds its LLM context from (one read per turn, D30), and what the
    /// transcript endpoint renders.
    ///
    /// The turn lock serialises turns per conversation, so the snapshot is stable for
    /// the in-flight turn: no other turn appends concurrently.
    async fn read_snapshot(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<ResolvedContext, ConversationError>;

    /// Append one turn's items to the snapshot at terminal time.
    ///
    /// Both the turn's input and output are appended, so the next turn's model
    /// context is complete without the caller re-supplying history. The items come
    /// from the execution side's final result — **never** derived by replaying the
    /// event stream for a reachable execution (INV-48). This is the single place
    /// durable content is written; on the runtime's completion path it is paired with
    /// `ResponseClaimSource::complete` in one transaction boundary (INV-34).
    ///
    /// A turn that ended without producing output (failed, cancelled, reaped) still
    /// archives its input, so the conversation chain is not left with a gap: `output_items`
    /// is legitimately empty in that case. Completed items are archived as they finished;
    /// a message that was still streaming is archived as an `ItemStatus::Incomplete`
    /// message reconstructed from its deltas, so the tokens the user already saw survive
    /// (INV-61).
    ///
    /// **Idempotent per `response_id`**: every terminal path may call this for the same
    /// response (the runtime's completion/failure funnel and the service layer's
    /// cancel/reap funnel can race). The store remembers the assigned turn index and
    /// returns it on a repeat instead of appending twice.
    ///
    /// Returns the turn's index (0-based, monotonically increasing).
    async fn append_turn(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        response_id: &ResponseId,
        commit: TurnCommit,
        now_ms: u64,
    ) -> Result<u64, ConversationError>;

    /// Move the tail pointer to `last`, after a turn reaches a terminal status.
    ///
    /// **Last write wins.** Two turns racing on the same conversation leave whichever
    /// finished last as the tail; the other's chain survives intact and addressable,
    /// it simply is not the tail any more. This is a deliberate choice: a
    /// compare-and-set here would reject the loser with a conflict status that
    /// upstream never returns, breaking callers that drive the conversation with an
    /// official SDK. Turns that need serialisation get it from [`TurnLock`], which
    /// refuses the second turn up front instead of letting both run and discarding
    /// one result.
    async fn advance(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        last: &ResponseId,
    ) -> Result<(), ConversationError>;
}

/// The per-conversation mutual-exclusion marker (D28).
///
/// Every method pairs a marker transition with the event that announces it, because
/// the two must land together or not at all — a crash between two separate calls
/// leaves either a busy conversation with nothing on the stream to explain it, or an
/// announced turn no marker is holding. Atomicity is a property of the store, so the
/// pairing lives in the port.
#[async_trait]
pub trait TurnLock: Send + Sync {
    /// Occupy the marker and emit `TurnStarted`, atomically.
    ///
    /// Contract:
    /// - `active` empty → set it to `response_id`, emit `TurnStarted`, return its seq
    /// - `active` already holds another response → [`ConversationError::Busy`] naming
    ///   the holder, and **write nothing** (no event, no partial state)
    /// - re-entering with the id that already holds the marker succeeds without a
    ///   second event, so an execution-side retry is harmless
    async fn acquire_active(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        response_id: &ResponseId,
        now_ms: u64,
    ) -> Result<u64, ConversationError>;

    /// Clear the marker and emit `TurnCompleted`, atomically.
    ///
    /// Must be called on **every** terminal path. Conditional on `response_id` still
    /// being the holder, and idempotent for the same id.
    async fn release_active(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        response_id: &ResponseId,
        status: ResponseStatus,
        now_ms: u64,
    ) -> Result<u64, ConversationError>;

    /// Release a marker held by `holder` without a `TurnCompleted`, for one situation
    /// only: the holder is already terminal but its marker was never released. No
    /// event is emitted — the terminal event was already emitted by whoever completed
    /// the response.
    ///
    /// Returns whether a marker was actually released. Conditional on `holder` still
    /// being the holder.
    async fn release_stale_active(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        holder: &ResponseId,
    ) -> Result<bool, ConversationError>;
}

/// The conversation's event stream (D28): turn boundaries, deletions and business
/// events in one sequence space.
///
/// This is the whole surface the SSE skeleton needs, which is the point of it being
/// its own trait.
#[async_trait]
pub trait ConversationEvents: Send + Sync {
    /// Append a non-turn event (`Business` or `ResponseDeleted`) and return its
    /// sequence number. Turn boundaries go through [`TurnLock`] instead, since those
    /// must be paired with the marker.
    async fn append_event(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        kind: ConversationEventKind,
        now_ms: u64,
    ) -> Result<u64, ConversationError>;

    /// Read events strictly after `starting_after`. `None` means "from the
    /// beginning" — necessary because 0 is a legitimate sequence number, so a
    /// sentinel would be ambiguous. `wait` allows a long poll for live continuation.
    async fn read_after(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        starting_after: Option<u64>,
        limit: usize,
        wait: Duration,
    ) -> Result<Vec<ConversationEvent>, ConversationError>;

    /// Set the per-conversation event-stream bound. Reaching it refuses the append
    /// rather than evicting the oldest events: dropping a turn boundary loses the
    /// only record that it happened.
    fn set_max_events(&self, limit: usize);
}

/// The whole conversation carrier.
///
/// A composition, not a fifth interface: assembly mounts one object, while each
/// consumer depends only on the facet it uses. The blanket impl means a backend
/// implements the four traits and gets this for free.
pub trait ConversationStore:
    ConversationRepo + ConversationSnapshots + TurnLock + ConversationEvents
{
}

impl<T> ConversationStore for T where
    T: ConversationRepo + ConversationSnapshots + TurnLock + ConversationEvents
{
}
