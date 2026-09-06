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
pub mod completions;
mod context;
mod conversation;
mod error;
mod events;
mod ids;
mod integrity_hmac;
mod ports;
pub mod protocol;
mod reconnect;
mod session;

pub use canonical::{canonical_items, canonical_json, canonical_output_text, nfc};
pub use completions::{
    assistant_text_message, CompletionsMessage, CompletionsOutcome, CompletionsRequest,
    FinishReason, RequestProvenance, ToolCall, ToolSpec,
};
pub use context::{
    ChainLimits, ResolvedContext, ResponseStatus, StoredResponse, Usage,
};
pub use conversation::Conversation;
pub use error::DomainError;
pub use events::{EventBody, ResponseEvent, ResponseEventKind};
pub use ids::{
    AgentId, Attempt, ConversationId, IdError, IdempotencyKey, NodeTag, ResponseId, SessionId,
    TenantId,
};
pub use integrity_hmac::{HmacSha256Integrity, ALG as INTEGRITY_ALG, KEY_ENV as INTEGRITY_KEY_ENV};
/// Re-exported for convenience: items are the unit both ports traffic in.
pub use protocol::{ContentPart, ResponseItem, Role};
pub use reconnect::JitteredBackoff;
pub use session::{LockState, Session, SessionEvent, SessionEventKind};

pub use ports::{
    validate_outcome, AbortedClaim, ClaimedResponse, Clock, CollectingSink, ContentIntegrity,
    ContextError, ContextStore, ConversationError, ConversationStore, CompletionsRequestScheduler,
    CompletionsSink, CreateOutcome, EventLogError, IntegrityError, LedgerError, MetricsSink,
    NoopToolExecutor, ResponseEventLog, ResponseLedger, SchedulerError, SessionError, SessionStore,
    SinkError, SinkVerdict, ToolError, ToolExecutor,
};
