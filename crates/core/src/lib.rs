//! Domain types, closed protocol subset and port traits for the response service.
//!
//! Boundary summary (see `docs/architecture/decisions.md` D20–D27):
//! - generation is still per-response; a conversation is a **pointer to the tail
//!   of a response chain**, not a container of items (D27)
//! - response items are persisted; the token-level event stream is not (D20 ④).
//!   The session layer's own stream is durable, but carries only low-frequency
//!   envelopes — turn boundaries and business events — never content (D26)
//! - stored output is committed by the execution side, never derived by
//!   replaying events (INV-48)

mod canonical;
mod context;
mod conversation;
mod error;
mod events;
mod ids;
mod integrity_hmac;
mod ports;
pub mod protocol;
mod provenance;
mod reconnect;

pub use canonical::{canonical_items, canonical_json, canonical_output_text, nfc};
pub use context::{
    ChainLimits, ResolvedContext, ResponseStatus, StoredResponse, Usage,
};
pub use conversation::{Conversation, ConversationEvent, ConversationEventKind};
pub use error::DomainError;
pub use events::{AppendEvent, EventBody, ResponseEvent, ResponseEventKind};
pub use ids::{
    AgentId, Attempt, ConversationId, IdError, IdempotencyKey, NodeTag, ResponseId, TenantId,
};
pub use integrity_hmac::{HmacSha256Integrity, ALG as INTEGRITY_ALG, KEY_ENV as INTEGRITY_KEY_ENV};
/// Re-exported for convenience: items are the unit both ports traffic in.
pub use protocol::{ContentPart, ResponseItem, Role};
pub use provenance::RequestProvenance;
pub use reconnect::JitteredBackoff;

pub use ports::{
    AbortedClaim, ClaimedResponse, ContentIntegrity, ContextError, ContextStore,
    ConversationError, ConversationStore, CreateOutcome, EventLogError, IntegrityError, LedgerError,
    MetricsSink, ResponseEventLog, ResponseLedger,
};
