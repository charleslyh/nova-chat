//! Capability-layer failures.
//!
//! # Why context failures are not conversation-store failures
//!
//! Chain resolution used to report itself through `ConversationError` — `ChainBroken`,
//! `ChainTooLong`, `NotStored`, `CrossTenant` — none of which any conversation store
//! ever returns. They were constructed *here*, in the capability layer, and put into a
//! port's error type. The effect was that a backend implementer had to read six
//! variants it could never produce, and `ConversationStore`'s contract stopped being
//! decidable from the port alone.
//!
//! Context resolution now reports [`ContextError`], and the port reports only what a
//! store can actually fail at.

use crate::ports::{ConversationError, EventLogError, LedgerError};
use crate::response::ResponseId;

/// Why the context a turn asked to inherit could not be assembled.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ContextError {
    /// The anchor does not exist, or its durable carrier is gone. Reported
    /// identically for "absent" and "foreign" so ids cannot be probed (SEC-2).
    #[error("chain broken at {0}")]
    ChainBroken(ResponseId),
    /// The referenced response was created with `store: false`, so it holds no
    /// durable snapshot to inherit (FR-18).
    #[error("referenced response was not stored")]
    NotStored,
    #[error("chain crosses tenant boundary")]
    CrossTenant,
    #[error("chain exceeds depth limit {limit}")]
    TooDeep { limit: usize },
    #[error("chain exceeds item limit {limit}")]
    TooManyItems { limit: usize },
    #[error("chain exceeds byte limit {limit}")]
    TooLarge { limit: usize },
}

/// Capability-layer error: the union of what the orchestration can hit. The ingress
/// layer maps each arm to its own status (INV-43).
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error(transparent)]
    Ledger(#[from] LedgerError),
    #[error(transparent)]
    EventLog(#[from] EventLogError),
    #[error(transparent)]
    Conversation(#[from] ConversationError),
    #[error(transparent)]
    Context(#[from] ContextError),
}
