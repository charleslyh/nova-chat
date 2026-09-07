use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::context::ResponseStatus;
use crate::ids::{ConversationId, ResponseId, SessionId, TenantId};
use crate::session::{Session, SessionEvent, SessionEventKind};

#[derive(Debug, Error, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionError {
    /// The session does not exist, or belongs to another tenant. One error for
    /// both so ids cannot be probed (SEC-2).
    #[error("not found")]
    NotFound,
    /// A turn is already in flight for this session. The caller is told, never
    /// silently queued behind the running turn.
    ///
    /// The holder is reported because "busy" alone is not actionable: a lock can
    /// outlive its holder — the process that took it may have been killed between
    /// the ledger transition and the release, or the release itself may have
    /// failed. Naming the holder lets the caller ask the ledger whether that
    /// response is still running and take the lock over if it is not, which is
    /// what makes a stuck lock recoverable without a timeout to guess at.
    #[error("session is busy with {holder}")]
    Busy { holder: ResponseId },
    /// The conversation already belongs to another session.
    ///
    /// Binding is exclusive so the turn lock actually guards the chain; a second
    /// session over the same conversation would be a second lock guarding
    /// nothing.
    #[error("conversation already belongs to another session")]
    ConversationTaken,
    #[error("capacity exceeded")]
    CapacityExceeded,
    /// Store is unreachable. Callers must **reject the write** rather than
    /// proceed without persisting (INV-46).
    #[error("unavailable")]
    Unavailable,
    #[error("read only")]
    ReadOnly,
    #[error("internal: {0}")]
    Internal(String),
}

/// Session metadata, the single-turn lock and the durable event stream, behind
/// **one** port.
///
/// Splitting the lock away from the stream is the obvious-looking factoring and
/// it is wrong. The compare-and-set that takes the lock and the `TurnStarted`
/// event that announces it must land together or not at all: a crash between two
/// separate calls leaves either a session locked with nothing on the stream to
/// explain it, or an announced turn no lock is holding. Both are permanent
/// disagreements between what every device shows and what the server believes,
/// and neither is repairable from the outside. Atomicity here is a property of
/// the store, so the store is where the pairing has to live.
///
/// Contrast with [`crate::ports::ContextStore`], which persists response items
/// and resolves chains. That port owns conversation *content*; this one owns
/// session *state and ordering* and never holds content — see
/// [`crate::session`].
#[async_trait]
pub trait SessionStore: Send + Sync {
    /// Persist a new session and emit `SessionCreated` at sequence 0.
    async fn create(&self, session: Session, now_ms: u64) -> Result<Session, SessionError>;

    async fn get(
        &self,
        tenant: &TenantId,
        id: &SessionId,
    ) -> Result<Option<Session>, SessionError>;

    /// The session that owns `conversation`, if any.
    ///
    /// This reverse lookup is what lets a plain, unmodified
    /// `POST /v1/responses { conversation: … }` participate in a session: the
    /// service finds the owning session and takes its turn lock, so multi-device
    /// delivery needs no field upstream does not have.
    ///
    /// It is a *function* rather than a list because the binding is exclusive —
    /// at most one session per conversation ([`SessionError::ConversationTaken`]).
    /// Two sessions over one conversation would mean two independent turn locks
    /// guarding the same chain, which is no lock at all.
    async fn get_by_conversation(
        &self,
        tenant: &TenantId,
        conversation: &ConversationId,
    ) -> Result<Option<Session>, SessionError>;

    /// Every session the tenant owns, newest first.
    ///
    /// Ordered by creation time so a UI can render a stable list without sorting
    /// client-side. The ordering contract lives here rather than in every caller,
    /// for the same reason the sequence-number contract lives in the port: it is a
    /// property the store can guarantee once and every consumer can rely on.
    async fn list(&self, tenant: &TenantId) -> Result<Vec<Session>, SessionError>;

    /// Delete a session and its events. Returns whether one was removed.
    async fn delete(&self, tenant: &TenantId, id: &SessionId) -> Result<bool, SessionError>;

    async fn delete_by_tenant(&self, tenant: &TenantId) -> Result<u64, SessionError>;

    /// Take the lock for `response_id` and append `TurnStarted`, atomically.
    ///
    /// Returns the assigned sequence number.
    ///
    /// Contract:
    /// - idle → busy is a compare-and-set; a concurrent caller gets
    ///   [`SessionError::Busy`] naming the holder, and **nothing is written** —
    ///   no event, no partial state
    /// - re-entering with the id that already holds the lock succeeds without
    ///   appending a second event, so an execution-side retry is harmless
    async fn begin_turn(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        response_id: &ResponseId,
        now_ms: u64,
    ) -> Result<u64, SessionError>;

    /// Release a lock held by `holder`, without appending a `TurnCompleted`.
    ///
    /// For one situation only: the holder is already terminal, but its lock was
    /// never released — the process died between the ledger transition and the
    /// release, or the release call itself failed. Since a lock can outlive its
    /// holder, something has to be able to take it back, or the session is
    /// unusable forever.
    ///
    /// No event is emitted **because the terminal event was already emitted** by
    /// whoever completed the response; appending a second one would put two
    /// terminal events for one turn on the stream and leave every subscriber to
    /// work out that they describe the same thing.
    ///
    /// Returns whether a lock was actually released. Releasing is conditional on
    /// `holder` still being the holder, so a caller that raced with a legitimate
    /// new turn cannot unlock it.
    async fn release_stale_lock(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        holder: &ResponseId,
    ) -> Result<bool, SessionError>;

    /// Release the lock and append `TurnCompleted`, atomically.
    ///
    /// Must be called on **every** terminal path (completed, failed, incomplete,
    /// cancelled, reaped); a path that skips it locks the session forever.
    ///
    /// Idempotent: calling it again for the same response is a no-op that
    /// returns the sequence number already assigned, because the execution side
    /// may re-enter after a retry or a restart.
    async fn end_turn(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        response_id: &ResponseId,
        status: ResponseStatus,
        now_ms: u64,
    ) -> Result<u64, SessionError>;

    /// Append an event and return its sequence number.
    ///
    /// Used for business events and `ResponseDeleted`. Turn boundaries go
    /// through [`SessionStore::begin_turn`] / [`SessionStore::end_turn`] instead,
    /// since those must be paired with the lock transition.
    async fn append_event(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        kind: SessionEventKind,
        now_ms: u64,
    ) -> Result<u64, SessionError>;

    /// Read events strictly after `starting_after`.
    ///
    /// `None` means "from the beginning" — 0 is a legitimate sequence number, so
    /// a sentinel would be ambiguous. The cursor is exclusive, matching
    /// [`crate::ports::ResponseEventLog::read_after`] so both streams are read
    /// with one rule (INV-11).
    ///
    /// `wait_ms` allows a long poll. This is what lets a subscriber replay
    /// durable history and then continue live in a single call: the reader keeps
    /// asking with the last sequence it saw, and the store blocks instead of
    /// returning empty. No separate snapshot endpoint is needed, and therefore
    /// no snapshot can go stale.
    async fn read_after(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        starting_after: Option<u64>,
        limit: usize,
        wait_ms: u64,
    ) -> Result<Vec<SessionEvent>, SessionError>;

    /// Liveness probe backing the refuse-writes degrade (INV-46).
    async fn health(&self) -> Result<(), SessionError>;

    /// Set the per-session event-stream bound.
    ///
    /// Reaching it refuses the append with [`SessionError::CapacityExceeded`];
    /// it never evicts the oldest events (INV-59) — a dropped turn boundary or
    /// business event would lose the only record that it happened.
    ///
    /// On the trait (rather than only a constructor knob) for the same reason
    /// [`crate::ports::ResponseLedger::set_pending_limit`] is: an operational
    /// bound must be changeable at runtime, and a conformance case can only
    /// exercise the capacity path by shrinking it below what a test produces.
    fn set_max_events_per_session(&self, limit: usize);
}
