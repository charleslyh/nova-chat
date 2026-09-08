use thiserror::Error;

use crate::shared::IdError;

/// Domain-level failures surfaced to the ingress layer.
///
/// Every variant maps to exactly one externally visible status so that no
/// failure can be silently downgraded (INV-43).
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DomainError {
    #[error("invalid transition: {0}")]
    InvalidTransition(String),
    #[error("not found")]
    NotFound,
    #[error("stale attempt")]
    StaleAttempt,
    #[error("duplicate")]
    Duplicate,
    /// Cursor or record past its retention window. Distinct from `NotFound`
    /// because the caller's remedy differs: expired is permanent and has no
    /// recovery path (INV-40).
    #[error("expired")]
    Expired,
    /// Tenant mismatch. Callers are told `NotFound` at the edge to avoid id
    /// enumeration (SEC-2); this variant exists for internal logging only.
    #[error("unauthorized")]
    Unauthorized,
    #[error("chain broken: {0}")]
    ChainBroken(String),
    #[error("chain too long")]
    ChainTooLong,
    #[error("chain too large")]
    ChainTooLarge,
    #[error("invalid id: {0}")]
    InvalidId(#[from] IdError),
    #[error("integrity mismatch")]
    IntegrityMismatch,
    #[error("unsupported item type: {0}")]
    UnsupportedItemType(String),
    #[error("blocked url: {0}")]
    BlockedUrl(String),
    #[error("read only")]
    ReadOnly,
    #[error("overloaded")]
    Overloaded,
    /// Context store unreachable — writes must be refused, never silently
    /// skipped (INV-46).
    #[error("store unavailable")]
    StoreUnavailable,
    // No `Busy`: session locks no longer exist (D20 ①).
}
