//! Responses 领域 + 能力层，不含任何具体存储适配器。
//!
//! 领域边界（见 `docs/architecture/decisions.md` D20–D30）：
//! - generation is still per-response; a conversation is a **pointer to the tail of a
//!   response chain**, not a container of items (D27)
//! - response items are persisted; the token-level event stream is not (D20 ④)
//! - stored output is committed by the execution side, never derived by replaying
//!   events (INV-48)
//!
//! 本 crate 承载领域类型、协议子集与 responses/conversations 用例编排（[`service`]）。
//! 它**不依赖任何具体存储适配器**：账本、事件缓冲、会话全部经 [`ports`] 的 trait 注入，
//! 由装配方（gateway）决定连哪个载体（D25）。HTTP 接入层在 gateway crate，后台维护循环
//! （sweeper）由 [`service::ResponsesService`] 自己持有，执行编排在 `nova-agent-runtime`。
//!
//! # 访问路径
//!
//! 每个类型只有**一条**公开路径。领域类型在 crate 根，协议子集在 [`protocol`]，端口在
//! [`ports`]，编排在 [`service`]，配置在 [`config`]。
//!
//! 唯一的例外是 [`ResponseItem`]、[`ContentPart`]、[`Role`]、[`Tool`]、[`ToolChoice`]:
//! 它们既是协议形状也是端口来往的领域值（记录存它们、快照存它们、事件带它们），所以在
//! 根上也重新导出一次。这是写明的取舍，不是遗漏。

mod canonical;
mod clock;
mod context;
mod conversation;
mod events;
mod identity;
mod integrity_hmac;
mod provenance;
mod response;
mod usage;

pub mod config;
pub mod ports;
pub mod protocol;
pub mod service;

pub use canonical::{canonical_items, canonical_json, canonical_output_text, nfc};
pub use clock::{Clock, SystemClock};
pub use context::{ContextEntry, ResolvedContext};
pub use conversation::{
    Conversation, ConversationEvent, ConversationEventKind, ConversationId, TurnCommit,
};
pub use events::{AppendEvent, EventBody, ResponseEvent, ResponseEventKind};
pub use identity::{AgentId, Attempt, IdError, IdempotencyKey, NodeTag, TenantId};
pub use integrity_hmac::{HmacSha256Integrity, ALG as INTEGRITY_ALG, KEY_ENV as INTEGRITY_KEY_ENV};
pub use provenance::RequestProvenance;
pub use response::{
    ContextAnchor, IntegrityTag, ModelParams, ResponseId, ResponseRecord, ResponseStatus, TurnSpec,
};
pub use usage::Usage;

/// Re-exported for convenience: these are the protocol shapes that are also the
/// domain values every port traffics in. See the module header.
pub use protocol::{ContentPart, ItemStatus, ResponseItem, Role, Tool, ToolChoice};
