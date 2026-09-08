use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::events::{AppendEvent, ResponseEvent};
use crate::context::ResponseId;

#[derive(Debug, Error, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventLogError {
    /// No buffer for this id on this node — either never created here, or the
    /// host process restarted.
    #[error("unknown response")]
    Unknown,
    /// Requested cursor is below the evicted watermark, or the retention window
    /// after terminal has elapsed.
    ///
    /// **There is no recovery path** (INV-40): callers must not be handed a
    /// partial view, and no snapshot/cold layer exists to fall back to.
    #[error("cursor expired")]
    Expired,
    #[error("stale attempt")]
    StaleAttempt,
    #[error("read only")]
    ReadOnly,
    /// Ring is full for this response; refuse rather than evict live data.
    #[error("capacity exceeded")]
    CapacityExceeded,
    #[error("internal: {0}")]
    Internal(String),
}

/// Per-response bounded event buffer.
///
/// Deliberately absent compared to the previous session-scoped channel:
/// `read_from`, gap reporting, trimming and any cold tier. Sequence numbers are
/// 0-based and contiguous (INV-11), which is exactly what makes those
/// mechanisms unnecessary — the only possible discontinuity is eviction, and
/// eviction must be reported explicitly.
#[async_trait]
pub trait ResponseEventLog: Send + Sync {
    /// Append and return the assigned sequence number.
    ///
    /// The input is an [`AppendEvent`]: it carries no `sequence_number`, because
    /// the number is this implementation's to assign (INV-11). A producer never
    /// invents one, and a backend never has to overwrite or trust one supplied
    /// by its caller.
    async fn append(&self, event: AppendEvent) -> Result<u64, EventLogError>;

    /// Read events strictly after `starting_after`.
    ///
    /// `None` means "from the beginning" — necessary because 0 is a legitimate
    /// sequence number, so a sentinel value would be ambiguous.
    ///
    /// `wait_ms` allows a long poll: implementations may block until either a
    /// matching event arrives or the budget elapses, then return what they have
    /// (possibly empty). This keeps first-token latency low without forcing the
    /// ingress layer into a tight polling loop.
    async fn read_after(
        &self,
        response_id: &ResponseId,
        starting_after: Option<u64>,
        limit: usize,
        wait_ms: u64,
    ) -> Result<Vec<ResponseEvent>, EventLogError>;

    /// Mark terminal and start the retention window.
    ///
    /// Takes `now_ms` explicitly rather than reaching for a clock, so the
    /// retention window is testable under a virtual clock (D15).
    async fn close(
        &self,
        response_id: &ResponseId,
        now_ms: u64,
        retain_ms: u64,
    ) -> Result<(), EventLogError>;

    /// Drop buffers whose retention window has elapsed. Returns the count.
    async fn sweep_expired(&self, now_ms: u64) -> Result<u64, EventLogError>;
}
