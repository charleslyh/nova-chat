//! Wire protocol between the mem-server (data owner) and its clients.
//!
//! This is the **data plane** contract: one request/response pair per port
//! operation. The carrier exposes the *shared* ledger / event log / context
//! operations only. The per-node runtime controls (`read_only`,
//! `pending_limit`) are **not** here — they are process-local admission state
//! held by the client adapter, exactly as `SqlResponseLedger` holds them as
//! atomics (FR-33 / INV-32 are per-node, not per-carrier).
//!
//! The control plane (fault injection) lives in [`crate::control`] and is a
//! separate surface, owned by the test controller.

use serde::{Deserialize, Serialize};

use std::collections::BTreeMap;

use nova_responses_core::{
    AbortedClaim, AgentId, Attempt, ChainLimits, ClaimedResponse, ContextError, Conversation,
    ConversationError, ConversationId, CreateOutcome, EventLogError, IdempotencyKey, LedgerError,
    ResolvedContext, ResponseEvent, ResponseEventKind, ResponseId, ResponseItem, ResponseStatus,
    Session, SessionError, SessionEvent, SessionEventKind, SessionId, StoredResponse, TenantId,
    Usage,
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
    pub body: nova_responses_core::EventBody,
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

/// A carrier-side failure, tagged by which port rejected the call.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum ProtoError {
    Ledger(LedgerError),
    EventLog(EventLogError),
    Context(ContextError),
    Conversation(ConversationError),
    Session(SessionError),
    /// A failure in the carrier itself (serialization, dispatch) rather than in
    /// a domain operation. Carried as text for debuggability.
    Internal(String),
}

/// A single data-plane request. One variant per shared port operation.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Request {
    // --- ledger ---
    LedgerCreate {
        record: StoredResponse,
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
    LedgerCheckAttempt {
        response_id: ResponseId,
        attempt: Attempt,
    },
    LedgerInFlight,

    // --- event log ---
    EventLogAppend {
        event: WireEvent,
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

    // --- context ---
    ContextPut {
        record: StoredResponse,
    },
    ContextAppendOutput {
        tenant: TenantId,
        response_id: ResponseId,
        items: Vec<ResponseItem>,
        usage: Usage,
        status: ResponseStatus,
        now_ms: u64,
    },
    ContextGet {
        tenant: TenantId,
        response_id: ResponseId,
    },
    ContextResolveChain {
        tenant: TenantId,
        from: ResponseId,
        limits: ChainLimits,
    },
    ContextDelete {
        tenant: TenantId,
        response_id: ResponseId,
    },
    ContextDeleteByTenant {
        tenant: TenantId,
    },
    ContextSweepExpired {
        now_ms: u64,
        limit: usize,
    },
    ContextHealth,

    // --- conversation (D27) ---
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
    ConversationHealth,

    // --- session (D26) ---
    //
    // `begin_turn` and `end_turn` are carried as their own operations rather than
    // as a lock write plus an append. Splitting them here would put the atomicity
    // the port promises on the wrong side of the wire, where a dropped connection
    // between the two halves would leave the session inconsistent.
    SessionCreate {
        session: Session,
        now_ms: u64,
    },
    SessionGet {
        tenant: TenantId,
        id: SessionId,
    },
    SessionGetByConversation {
        tenant: TenantId,
        conversation: ConversationId,
    },
    SessionDelete {
        tenant: TenantId,
        id: SessionId,
    },
    SessionDeleteByTenant {
        tenant: TenantId,
    },
    SessionBeginTurn {
        tenant: TenantId,
        id: SessionId,
        response_id: ResponseId,
        now_ms: u64,
    },
    SessionEndTurn {
        tenant: TenantId,
        id: SessionId,
        response_id: ResponseId,
        status: ResponseStatus,
        now_ms: u64,
    },
    SessionReleaseStaleLock {
        tenant: TenantId,
        id: SessionId,
        holder: ResponseId,
    },
    SessionAppendEvent {
        tenant: TenantId,
        id: SessionId,
        kind: SessionEventKind,
        now_ms: u64,
    },
    SessionReadAfter {
        tenant: TenantId,
        id: SessionId,
        starting_after: Option<u64>,
        limit: usize,
        wait_ms: u64,
    },
    SessionHealth,
    SessionSetMaxEvents {
        limit: usize,
    },
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
    Get(Option<StoredResponse>),
    CheckAttempt,
    InFlight(usize),

    EventLogAppend(u64),
    EventLogReadAfter(Vec<WireEvent>),
    EventLogClose,
    EventLogSweepExpired(u64),

    ContextPut,
    ContextAppendOutput,
    ContextGet(Option<StoredResponse>),
    ContextResolveChain(ResolvedContext),
    ContextDelete(bool),
    ContextDeleteByTenant(u64),
    ContextSweepExpired(u64),
    ContextHealth,

    ConversationCreate(Conversation),
    ConversationGet(Option<Conversation>),
    ConversationUpdateMetadata(Conversation),
    ConversationDelete(bool),
    ConversationDeleteByTenant(u64),
    ConversationAdvance,
    ConversationHealth,

    SessionCreate(Session),
    SessionGet(Option<Session>),
    SessionDelete(bool),
    SessionDeleteByTenant(u64),
    SessionReleaseStaleLock(bool),
    /// Assigned sequence number, shared by the three appending operations.
    SessionSeq(u64),
    SessionReadAfter(Vec<SessionEvent>),
    SessionHealth,
    SessionSetMaxEvents,

    Err(ProtoError),
}
