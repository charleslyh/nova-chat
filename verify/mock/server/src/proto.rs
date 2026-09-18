//! Wire protocol between the mem-server (data owner) and its clients.
//!
//! This is the **data plane** contract: one request/response pair per port
//! operation. The carrier exposes the *shared* ledger / event log / context
//! operations only. The per-node storage degrade switch (`read_only`) is
//! **not** here — it is process-local state held by the client adapter.
//!
//! The control plane (fault injection) lives in [`crate::control`] and is a
//! separate surface, owned by the test controller.

use serde::{Deserialize, Serialize};

use std::collections::BTreeMap;
use std::time::Duration;

use nova_responses::{
    AgentId, AppendEvent, Attempt, Conversation, ConversationEvent, ConversationEventKind,
    ConversationId, IdempotencyKey, ResolvedContext, ResponseEvent, ResponseEventKind, ResponseId,
    ResponseRecord, ResponseStatus, TenantId, TurnCommit, Usage,
};
use nova_responses::ports::{AbortedClaim, ClaimedResponse, ConversationError, CreateOutcome, EventLogError, LedgerError};
use nova_responses::protocol::MetadataValue;

/// Internal wire form of an event to append.
///
/// The domain's own serialisation is the **SSE** shape: it omits `response_id` (which
/// lives in the URL) and `attempt` (an internal fence). A cross-process carrier has to
/// preserve both, so it needs its own round-trippable form — this one.
///
/// There is a single wire type for both directions, with the sequence number optional:
/// absent on the way in (the carrier assigns it, INV-11), present on the way back. Two
/// near-identical structs plus four hand-written conversions expressed exactly the same
/// thing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WireEvent {
    pub response_id: ResponseId,
    pub attempt: Option<Attempt>,
    /// `None` on an append, `Some` once the log has numbered it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<u64>,
    pub kind: ResponseEventKind,
    #[serde(flatten)]
    pub body: nova_responses::EventBody,
}

impl From<ResponseEvent> for WireEvent {
    fn from(e: ResponseEvent) -> Self {
        let sequence_number = Some(e.sequence_number());
        let mut wire = WireEvent::from(e.into_append());
        wire.sequence_number = sequence_number;
        wire
    }
}

impl From<AppendEvent> for WireEvent {
    fn from(e: AppendEvent) -> Self {
        WireEvent {
            response_id: e.response_id().clone(),
            attempt: e.attempt(),
            sequence_number: None,
            kind: e.kind(),
            body: e.body().clone(),
        }
    }
}

impl From<WireEvent> for AppendEvent {
    fn from(w: WireEvent) -> Self {
        AppendEvent::from_parts(w.response_id, w.kind, w.attempt, w.body)
    }
}

impl WireEvent {
    /// Rebuild the numbered form. The number is required here: a read that lost it would
    /// break every cursor downstream, so its absence is a carrier defect, not a value to
    /// invent.
    pub fn into_response_event(self) -> Result<ResponseEvent, ProtoError> {
        let Some(seq) = self.sequence_number else {
            return Err(ProtoError::Internal(
                "carrier returned an event with no sequence number".into(),
            ));
        };
        Ok(AppendEvent::from(self).with_seq(seq))
    }
}

/// A carrier-side failure, tagged by which port rejected the call.
///
/// `Display` is derived rather than left to callers: a client that has to fold this into
/// its own port error needs a message, and every call site formatting the `Debug` shape
/// by hand would leak Rust syntax into an error a human reads.
#[derive(Debug, PartialEq, Serialize, Deserialize, thiserror::Error)]
pub enum ProtoError {
    #[error(transparent)]
    Ledger(LedgerError),
    #[error(transparent)]
    EventLog(EventLogError),
    #[error(transparent)]
    Conversation(ConversationError),
    /// A failure in the carrier itself (serialization, dispatch) rather than in a domain
    /// operation. Carried as text for debuggability.
    #[error("carrier: {0}")]
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
        exec_ttl: Duration,
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
        heartbeat_ttl: Duration,
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

    // --- event log ---
    EventLogAppend {
        event: WireEvent,
    },
    EventLogReadAfter {
        response_id: ResponseId,
        starting_after: Option<u64>,
        limit: usize,
        wait: Duration,
    },
    EventLogClose {
        response_id: ResponseId,
        now_ms: u64,
        retain: Duration,
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
        metadata: BTreeMap<String, MetadataValue>,
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
        wait: Duration,
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
        /// The commit as one value, not five fields the receiver has to reassemble into
        /// the very struct the sender took apart.
        commit: TurnCommit,
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
