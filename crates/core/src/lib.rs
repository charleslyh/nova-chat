//! Domain types, closed protocol subset and port traits for the response service.
//!
//! Boundary summary (see `docs/architecture/decisions.md` D20–D22):
//! - the externally visible resource is a **single response**, not a thread
//! - response items are persisted; the incremental event stream is not
//! - stored output is committed by the execution side, never derived by
//!   replaying events (INV-48)

mod canonical;
mod context;
mod error;
mod events;
mod ids;
mod integrity_hmac;
mod ports;
pub mod protocol;
mod reconnect;

pub use canonical::{canonical_items, canonical_json, canonical_output_text, nfc};
pub use context::{
    ChainLimits, ResolvedContext, ResponseStatus, StoredResponse, Usage,
};
pub use error::DomainError;
pub use events::{ResponseEvent, ResponseEventKind};
pub use ids::{AgentId, Attempt, IdError, IdempotencyKey, NodeTag, ResponseId, TenantId};
pub use integrity_hmac::{HmacSha256Integrity, ALG as INTEGRITY_ALG, KEY_ENV as INTEGRITY_KEY_ENV};
/// Re-exported for convenience: items are the unit both ports traffic in.
pub use protocol::{ContentPart, ResponseItem, Role};
pub use reconnect::JitteredBackoff;

pub use ports::{
    AbortedClaim, ClaimedResponse, Clock, ContentIntegrity, ContextError, ContextStore,
    CreateOutcome, EventLogError, IntegrityError, LedgerError, MetricsSink, ResponseEventLog,
    ResponseLedger,
};
