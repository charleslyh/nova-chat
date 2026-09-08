//! Responses 领域 + 能力层 + HTTP 接入层 + 后台维护。
//!
//! 领域边界（见 `docs/architecture/decisions.md` D20–D28）：
//! - generation is still per-response; a conversation is a **pointer to the tail
//!   of a response chain**, not a container of items (D27)
//! - response items are persisted; the token-level event stream is not (D20 ④)
//! - stored output is committed by the execution side, never derived by
//!   replaying events (INV-48)
//!
//! 本 crate 承载 responses 用例编排（无 axum 的 [`service`]）以及 sweeper。它
//! **不依赖任何具体存储适配器**：账本、上下文、事件缓冲、会话全部经端口 trait
//! 注入，由装配方（gateway）决定连哪个载体（D25）。HTTP 接入层在 gateway crate。

mod canonical;
mod context;
mod conversation;
mod domain_error;
mod events;
mod integrity_hmac;
mod ports;
pub mod protocol;
mod provenance;
mod shared;

pub mod config;
pub mod metrics;
pub mod service;

pub use canonical::{canonical_items, canonical_json, canonical_output_text, nfc};
pub use context::{ChainLimits, ResolvedContext, ResponseId, ResponseStatus, StoredResponse, Usage};
pub use conversation::{Conversation, ConversationEvent, ConversationEventKind, ConversationId};
pub use domain_error::DomainError;
pub use events::{AppendEvent, EventBody, ResponseEvent, ResponseEventKind};
pub use shared::{AgentId, Attempt, IdError, IdempotencyKey, NodeTag, TenantId};
pub use integrity_hmac::{HmacSha256Integrity, ALG as INTEGRITY_ALG, KEY_ENV as INTEGRITY_KEY_ENV};
/// Re-exported for convenience: items are the unit both ports traffic in.
pub use protocol::{ContentPart, ResponseItem, Role};
pub use provenance::RequestProvenance;

pub use ports::{
    AbortedClaim, ClaimedResponse, ContentIntegrity, ContextError, ContextStore,
    ConversationError, ConversationStore, CreateOutcome, EventLogError, IntegrityError, LedgerError,
    MetricsSink, ResponseEventLog, ResponseLedger,
};

pub use config::{Config, RawConfig};
pub use metrics::CountingMetrics;
pub use service::{ContextSource, ConversationsService, CreateResult, ResponsesService, ServiceError};
