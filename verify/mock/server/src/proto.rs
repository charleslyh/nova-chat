//! Wire protocol between the mem-server (data owner) and its clients.
//!
//! This is the **data plane** contract: one request/response pair per port
//! operation. The carrier exposes the *shared* ledger / event log / context
//! operations only. The per-node runtime controls (`read_only`,
//! `pending_limit`) are **not** here — they are process-local admission state
//! held by the client adapter as atomics (FR-33 / INV-32 are per-node, not
//! per-carrier).
//!
//! The control plane (fault injection) lives in [`crate::control`] and is a
//! separate surface, owned by the test controller.

use serde::{Deserialize, Serialize};

use std::collections::BTreeMap;

use nova_responses::{
    AbortedClaim, AgentId, AppendEvent, Attempt, ClaimedResponse, Conversation, ConversationError,
    ConversationEvent, ConversationEventKind, ConversationId, CreateOutcome, EventLogError,
    IdempotencyKey, LedgerError, ResolvedContext, ResponseEvent, ResponseEventKind, ResponseId,
    ResponseItem, ResponseRecord, ResponseStatus, TenantId, Usage,
};

/// Internal wire form of a stream event.
///
/// `ResponseEvent`'s public `Serialize` deliberately omits `response_id` and
/// `attempt` (they are internal fence/owner state, never on the SSE wire), so a
/// cross-process carrier needs its own round-trippable representation that
/// preserves both.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WireEvent {
    pub response_id: ResponseId,
    pub attempt: Option<Attempt>,
    pub sequence_number: u64,
    pub kind: ResponseEventKind,
    #[serde(flatten)]
    pub body: nova_responses::EventBody,
}

impl From<ResponseEvent> for WireEvent {
    fn from(e: ResponseEvent) -> Self {
        WireEvent {
            response_id: e.response_id,
            attempt: e.attempt,
            sequence_number: e.sequence_number,
            kind: e.kind,
            body: e.body,
        }
    }
}

impl From<WireEvent> for ResponseEvent {
    fn from(w: WireEvent) -> Self {
        ResponseEvent {
            response_id: w.response_id,
            attempt: w.attempt,
            sequence_number: w.sequence_number,
            kind: w.kind,
            body: w.body,
        }
    }
}

/// Internal wire form of an event to append — the append-input counterpart of
/// [`WireEvent`], with no `sequence_number` (the carrier assigns it, INV-11).
///
/// `AppendEvent`'s public `Serialize` omits `response_id` and `attempt`, so the
/// cross-process carrier needs its own round-trippable form that preserves both,
/// exactly as `WireEvent` does for the read path. The two are split on purpose:
/// an append carries no number, a read returns one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppendWireEvent {
    pub response_id: ResponseId,
    pub attempt: Option<Attempt>,
    pub kind: ResponseEventKind,
    #[serde(flatten)]
    pub body: nova_responses::EventBody,
}

impl From<AppendEvent> for AppendWireEvent {
    fn from(e: AppendEvent) -> Self {
        AppendWireEvent {
            response_id: e.response_id,
            attempt: e.attempt,
            kind: e.kind,
            body: e.body,
        }
    }
}

impl From<AppendWireEvent> for AppendEvent {
    fn from(w: AppendWireEvent) -> Self {
        AppendEvent {
            response_id: w.response_id,
            kind: w.kind,
            attempt: w.attempt,
            body: w.body,
        }
    }
}

/// A carrier-side failure, tagged by which port rejected the call.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum ProtoError {
    Ledger(LedgerError),
    EventLog(EventLogError),
    Conversation(ConversationError),
    /// A failure in the carrier itself (serialization, dispatch) rather than in
    /// a domain operation. Carried as text for debuggability.
    Internal(String),
}

/// A single data-plane request. One variant per shared port operation.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Request {
    // --- ledger ---
    LedgerCreate {
        // Boxed: `ResponseRecord` dwarfs every other variant's payload, and
        // boxing keeps the enum small on the wire-adjacent match paths.
        record: Box<ResponseRecord>,
        idempotency_key: IdempotencyKey,
        now_ms: u64,
    },
    LedgerClaim {
        agent_id: AgentId,
        now_ms: u64,
        exec_ttl_ms: u64,
    },
    LedgerHeartbeat {
        agent_id: AgentId,
        now_ms: u64,
    },
    LedgerComplete {
        response_id: ResponseId,
        expected_attempt: Attempt,
        status: ResponseStatus,
        usage: Usage,
        now_ms: u64,
    },
    LedgerCancel {
        tenant: TenantId,
        response_id: ResponseId,
        now_ms: u64,
    },
    LedgerReap {
        now_ms: u64,
        heartbeat_ttl_ms: u64,
    },
    LedgerRecordPartialUsage {
        response_id: ResponseId,
        attempt: Attempt,
        usage: Usage,
    },
    LedgerGet {
        response_id: ResponseId,
    },
    LedgerDelete {
        response_id: ResponseId,
    },
    LedgerDeleteByTenant {
        tenant: TenantId,
    },
    LedgerCheckAttempt {
        response_id: ResponseId,
        attempt: Attempt,
    },
    LedgerInFlight,

    // --- event log ---
    EventLogAppend {
        event: AppendWireEvent,
    },
    EventLogReadAfter {
        response_id: ResponseId,
        starting_after: Option<u64>,
        limit: usize,
        wait_ms: u64,
    },
    EventLogClose {
        response_id: ResponseId,
        now_ms: u64,
        retain_ms: u64,
    },
    EventLogSweepExpired {
        now_ms: u64,
    },
    EventLogRemove {
        response_id: ResponseId,
    },

    // --- conversation (D28 + D30) ---
    ConversationCreate {
        conversation: Conversation,
    },
    ConversationGet {
        tenant: TenantId,
        id: ConversationId,
    },
    ConversationUpdateMetadata {
        tenant: TenantId,
        id: ConversationId,
        metadata: BTreeMap<String, String>,
    },
    ConversationDelete {
        tenant: TenantId,
        id: ConversationId,
    },
    ConversationDeleteByTenant {
        tenant: TenantId,
    },
    ConversationAdvance {
        tenant: TenantId,
        id: ConversationId,
        last: ResponseId,
    },
    ConversationAcquireActive {
        tenant: TenantId,
        id: ConversationId,
        response_id: ResponseId,
        now_ms: u64,
    },
    ConversationReleaseActive {
        tenant: TenantId,
        id: ConversationId,
        response_id: ResponseId,
        status: ResponseStatus,
        now_ms: u64,
    },
    ConversationReleaseStaleActive {
        tenant: TenantId,
        id: ConversationId,
        holder: ResponseId,
    },
    ConversationAppendEvent {
        tenant: TenantId,
        id: ConversationId,
        kind: ConversationEventKind,
        now_ms: u64,
    },
    ConversationReadAfter {
        tenant: TenantId,
        id: ConversationId,
        starting_after: Option<u64>,
        limit: usize,
        wait_ms: u64,
    },
    ConversationList {
        tenant: TenantId,
    },
    ConversationReadSnapshot {
        tenant: TenantId,
        id: ConversationId,
    },
    ConversationAppendTurn {
        tenant: TenantId,
        id: ConversationId,
        response_id: ResponseId,
        input_items: Vec<ResponseItem>,
        output_items: Vec<ResponseItem>,
        reasoning: Option<String>,
        usage: Usage,
        status: ResponseStatus,
        now_ms: u64,
    },
    ConversationHealth,
}

/// A single data-plane response. Success variants carry the typed result; the
/// single `Err` variant carries a [`ProtoError`].
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Response {
    Create(CreateOutcome),
    Claim(Option<ClaimedResponse>),
    Heartbeat,
    Complete,
    Cancel,
    Reap(Vec<AbortedClaim>),
    RecordPartialUsage,
    Get(Option<ResponseRecord>),
    Delete(bool),
    DeleteByTenant(u64),
    CheckAttempt,
    InFlight(usize),

    EventLogAppend(u64),
    EventLogReadAfter(Vec<WireEvent>),
    EventLogClose,
    EventLogSweepExpired(u64),
    EventLogRemove,

    ConversationCreate(Conversation),
    ConversationGet(Option<Conversation>),
    ConversationUpdateMetadata(Conversation),
    ConversationDelete(bool),
    ConversationDeleteByTenant(u64),
    ConversationAdvance,
    ConversationAcquireActive(u64),
    ConversationReleaseActive(u64),
    ConversationReleaseStaleActive(bool),
    ConversationAppendEvent(u64),
    ConversationReadAfter(Vec<ConversationEvent>),
    ConversationList(Vec<Conversation>),
    ConversationReadSnapshot(ResolvedContext),
    ConversationAppendTurn(u64),
    ConversationHealth,

    Err(ProtoError),
}
