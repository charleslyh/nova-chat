//! Traceability for one execution attempt.
//!
//! Moved here from the (removed) `completions` module: provenance is a domain
//! concept — it names which response and which fencing attempt a unit of work
//! belongs to — not a completions-provider detail. It travels on the agent task
//! handed to an [`crate::AgentRunner`] implementation (defined in
//! `nova-agent-runtime`) and is used for logging, cost attribution and deadline
//! enforcement.

use serde::{Deserialize, Serialize};

/// Where one execution attempt came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestProvenance {
    /// The response being produced, as its wire id string.
    pub response_id: String,
    /// Fencing token. A runner that ignores this cannot tell its work was
    /// superseded, and will keep spending tokens on an abandoned attempt.
    pub attempt: u64,
    /// Wall-clock milliseconds after which this attempt is forfeit.
    pub exec_deadline_ms: u64,
}
