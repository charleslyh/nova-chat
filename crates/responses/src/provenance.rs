//! Traceability for one execution attempt.
//!
//! Provenance is a domain concept — it names which response and which fencing
//! attempt a unit of work belongs to — not a provider detail. It travels on the task
//! handed to a runner (defined in `nova-agent-runtime`) and is used for logging, cost
//! attribution and deadline enforcement.

use serde::{Deserialize, Serialize};

use crate::identity::Attempt;
use crate::response::ResponseId;

/// Where one execution attempt came from.
///
/// Derived from a claim rather than assembled by hand — see
/// [`crate::ports::ClaimedResponse::provenance`] — so the attempt here is always the
/// one the ledger actually raised.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestProvenance {
    /// The response being produced.
    pub response_id: ResponseId,
    /// Fencing token. A runner that ignores this cannot tell its work was superseded,
    /// and will keep spending tokens on an abandoned attempt.
    pub attempt: Attempt,
    /// Wall-clock milliseconds after which this attempt is forfeit.
    pub exec_deadline_ms: u64,
}
