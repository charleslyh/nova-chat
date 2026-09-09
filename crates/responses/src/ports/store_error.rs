//! Infrastructure errors shared by every storage port.
//!
//! `ReadOnly`, `Unavailable` and `Internal` mean the same thing regardless of
//! which carrier failed, so they live here once rather than being redefined in
//! `ConversationError`, `LedgerError` and `EventLogError` — three copies of one
//! fact drift apart the moment one port adds a fourth infrastructure failure.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Failures that are about the storage layer itself, not about the domain
/// operation the port was asked to perform.
#[derive(Debug, Error, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoreError {
    /// Read-only degrade: reads keep working, writes are refused (INV-32).
    #[error("read only")]
    ReadOnly,
    /// The carrier is unreachable; writes must be refused, never skipped
    /// (INV-46).
    #[error("unavailable")]
    Unavailable,
    /// A defect in the carrier or the transport, not a caller error.
    #[error("internal: {0}")]
    Internal(String),
}
