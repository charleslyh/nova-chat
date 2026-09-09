use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::events::{AppendEvent, ResponseEvent};
use crate::response::ResponseId;

use super::store_error::StoreError;

#[derive(Debug, Error, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventLogError {
    /// No buffer for this id — either never created, or the host process restarted.
    #[error("unknown response")]
    Unknown,
    /// Requested cursor is below the evicted watermark, or the retention window after
    /// terminal has elapsed.
    ///
    /// **There is no recovery path** (INV-40): callers must not be handed a partial
    /// view, and no snapshot/cold layer exists to fall back to.
    #[error("cursor expired")]
    Expired,
    #[error("stale attempt")]
    StaleAttempt,
    /// Ring is full for this response; refuse rather than evict live data.
    #[error("capacity exceeded")]
    CapacityExceeded,
    /// Infrastructure failure (read-only / internal).
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// Per-response bounded event buffer.
///
/// Under D30 this is the **short-lived carrier of a response's own record**: a
/// completed response's object (including its output) is reconstructable by replaying
/// this stream until the retention window elapses, after which retrieval yields
/// `Expired` (no cold tier to fall back to, INV-40). It never substitutes for the
/// durable conversation snapshot.
///
/// Deliberately absent compared to the previous session-scoped channel: `read_from`,
/// gap reporting, trimming and any cold tier. Sequence numbers are 0-based and
/// contiguous (INV-11), which is exactly what makes those mechanisms unnecessary —
/// the only possible discontinuity is eviction, and eviction must be reported
/// explicitly.
#[async_trait]
pub trait ResponseEventLog: Send + Sync {
    /// Append and return the assigned sequence number.
    ///
    /// The input is an [`AppendEvent`]: it carries no sequence number, because the
    /// number is this implementation's to assign (INV-11). A producer never invents
    /// one, and a backend never has to overwrite or trust one supplied by its caller.
    async fn append(&self, event: AppendEvent) -> Result<u64, EventLogError>;

    /// Read events strictly after `starting_after`.
    ///
    /// `None` means "from the beginning" — necessary because 0 is a legitimate
    /// sequence number, so a sentinel value would be ambiguous.
    ///
    /// `wait` allows a long poll: implementations may block until either a matching
    /// event arrives or the budget elapses, then return what they have (possibly
    /// empty). This keeps first-token latency low without forcing the ingress layer
    /// into a tight polling loop. It is a `Duration` rather than a bare number beside
    /// `limit`, so the two cannot be transposed.
    async fn read_after(
        &self,
        response_id: &ResponseId,
        starting_after: Option<u64>,
        limit: usize,
        wait: Duration,
    ) -> Result<Vec<ResponseEvent>, EventLogError>;

    /// Mark terminal and start the retention window.
    ///
    /// Takes `now_ms` explicitly rather than reaching for a clock, so the retention
    /// window is testable under a virtual clock (D15).
    async fn close(
        &self,
        response_id: &ResponseId,
        now_ms: u64,
        retain: Duration,
    ) -> Result<(), EventLogError>;

    /// Drop buffers whose retention window has elapsed. Returns the count.
    async fn sweep_expired(&self, now_ms: u64) -> Result<u64, EventLogError>;

    /// Remove a response's buffer immediately (record-level delete, D30).
    async fn remove(&self, response_id: &ResponseId) -> Result<(), EventLogError>;
}
