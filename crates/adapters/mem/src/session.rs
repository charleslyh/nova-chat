//! Session store over the shared in-memory state (D26).
//!
//! Scope: **verification only** (L0–L2). The sql adapter is the production
//! carrier; this exists so the same port contract can be asserted without a
//! database (D17).
//!
//! Every mutation happens inside one critical section of the shared mutex, which
//! is what makes the lock transition and its event a single step here — the same
//! guarantee the SQL adapter gets from a transaction.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nova_responses_core::{
    ConversationId, LockState, ResponseId, ResponseStatus, Session, SessionError, SessionEvent,
    SessionEventKind, SessionId, SessionStore, TenantId,
};
use tokio::sync::Notify;

use crate::store::{Inner, MemStore, SessionRow};

pub struct MemSessionStore {
    store: Arc<MemStore>,
    /// Wakes long-polling readers. Session-agnostic, like the per-response event
    /// log's: a spurious wake costs one re-check of a `Vec` index, while a
    /// per-session notifier would cost a map of them to keep in step with
    /// creation and deletion.
    notify: Notify,
}

impl MemSessionStore {
    pub fn new(store: Arc<MemStore>) -> Self {
        Self {
            store,
            notify: Notify::new(),
        }
    }

    fn guard_available(&self) -> Result<(), SessionError> {
        if self.store.is_unavailable() {
            // Callers must refuse the write, never proceed unstored (INV-46).
            return Err(SessionError::Unavailable);
        }
        Ok(())
    }

    fn guard_writable(&self) -> Result<(), SessionError> {
        self.guard_available()?;
        if self.store.is_read_only() {
            return Err(SessionError::ReadOnly);
        }
        Ok(())
    }

    pub fn session_count(&self) -> usize {
        self.store.lock().sessions.len()
    }

    /// Resolve a session for mutation, treating a foreign tenant as absent
    /// (SEC-2).
    fn row_mut<'a>(
        inner: &'a mut Inner,
        tenant: &TenantId,
        id: &SessionId,
    ) -> Result<&'a mut SessionRow, SessionError> {
        match inner.sessions.get_mut(id) {
            None => Err(SessionError::NotFound),
            Some(row) if &row.session.tenant_id != tenant => Err(SessionError::NotFound),
            Some(row) => Ok(row),
        }
    }
}

/// Append `kind` to `row` and return the assigned sequence number.
///
/// The only place a sequence number is minted, so contiguity cannot be broken by
/// one call site forgetting to bump the counter.
fn append_locked(
    row: &mut SessionRow,
    kind: SessionEventKind,
    now_ms: u64,
    max_events: usize,
) -> Result<u64, SessionError> {
    if row.events.len() >= max_events {
        // Refuse rather than evict: dropping a turn boundary or a business event
        // would lose the only record that it happened.
        return Err(SessionError::CapacityExceeded);
    }
    let seq = row.next_seq;
    row.next_seq += 1;
    row.events.push(SessionEvent {
        session_id: row.session.id.clone(),
        seq,
        kind,
        ts_ms: now_ms,
    });
    Ok(seq)
}

#[async_trait]
impl SessionStore for MemSessionStore {
    async fn create(&self, session: Session, now_ms: u64) -> Result<Session, SessionError> {
        self.guard_writable()?;
        let created = {
            let mut g = self.store.lock();
            if g.sessions.len() >= self.store.max_sessions() {
                return Err(SessionError::CapacityExceeded);
            }
            // Exclusive binding, checked inside the critical section so two
            // concurrent creates cannot both win. The SQL adapter gets the same
            // guarantee from a unique index.
            if g.sessions.values().any(|row| {
                row.session.tenant_id == session.tenant_id
                    && row.session.conversation_id == session.conversation_id
            }) {
                return Err(SessionError::ConversationTaken);
            }
            let mut row = SessionRow {
                session: session.clone(),
                next_seq: 0,
                events: Vec::new(),
                lock_seq: None,
                last_completed: None,
            };
            // Sequence 0 exists from the start, so a subscriber can tell "stream
            // begins here" from "nothing has happened yet".
            append_locked(
                &mut row,
                SessionEventKind::SessionCreated,
                now_ms,
                self.store.max_events_per_session(),
            )?;
            let created = row.session.clone();
            g.insert_session(row);
            created
        };
        self.notify.notify_waiters();
        Ok(created)
    }

    async fn get(
        &self,
        tenant: &TenantId,
        id: &SessionId,
    ) -> Result<Option<Session>, SessionError> {
        self.guard_available()?;
        let g = self.store.lock();
        Ok(match g.sessions.get(id) {
            None => None,
            Some(row) if &row.session.tenant_id != tenant => None,
            Some(row) => Some(row.session.clone()),
        })
    }

    async fn get_by_conversation(
        &self,
        tenant: &TenantId,
        conversation: &ConversationId,
    ) -> Result<Option<Session>, SessionError> {
        self.guard_available()?;
        let g = self.store.lock();
        Ok(g.sessions
            .values()
            .find(|row| {
                &row.session.tenant_id == tenant && &row.session.conversation_id == conversation
            })
            .map(|row| row.session.clone()))
    }

    async fn list(&self, tenant: &TenantId) -> Result<Vec<Session>, SessionError> {
        self.guard_available()?;
        let g = self.store.lock();
        let mut sessions: Vec<Session> = g
            .sessions_by_tenant
            .get(tenant)
            .map(|set| {
                set.iter()
                    .filter_map(|id| g.sessions.get(id).map(|row| row.session.clone()))
                    .collect()
            })
            .unwrap_or_default();
        // Newest first: a tie cannot happen (created_at is monotonic), but the id
        // fallback keeps the order total even across a clock that is not.
        sessions.sort_by(|a, b| {
            b.created_at_ms
                .cmp(&a.created_at_ms)
                .then_with(|| b.id.cmp(&a.id))
        });
        Ok(sessions)
    }

    async fn delete(&self, tenant: &TenantId, id: &SessionId) -> Result<bool, SessionError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        match g.sessions.get(id) {
            None => Ok(false),
            Some(row) if &row.session.tenant_id != tenant => Ok(false),
            Some(_) => Ok(g.remove_session(id).is_some()),
        }
    }

    async fn delete_by_tenant(&self, tenant: &TenantId) -> Result<u64, SessionError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        let ids: Vec<SessionId> = g
            .sessions_by_tenant
            .get(tenant)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default();
        let mut removed = 0u64;
        for id in ids {
            if g.remove_session(&id).is_some() {
                removed += 1;
            }
        }
        Ok(removed)
    }

    async fn begin_turn(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        response_id: &ResponseId,
        now_ms: u64,
    ) -> Result<u64, SessionError> {
        self.guard_writable()?;
        let max_events = self.store.max_events_per_session();
        let seq = {
            let mut g = self.store.lock();
            let row = Self::row_mut(&mut g, tenant, id)?;

            match row.session.lock_state.holder() {
                // Re-entry by the holder: return the sequence already assigned
                // instead of announcing the same turn twice.
                Some(holder) if holder == response_id => {
                    return row.lock_seq.ok_or_else(|| {
                        SessionError::Internal(
                            "session is busy but no sequence was recorded for the turn".into(),
                        )
                    });
                }
                Some(holder) => {
                    return Err(SessionError::Busy {
                        holder: holder.clone(),
                    })
                }
                None => {}
            }

            // Append first: if the bound is hit, the lock has not moved and the
            // caller sees a clean refusal rather than a session locked with
            // nothing on the stream.
            let seq = append_locked(
                row,
                SessionEventKind::TurnStarted {
                    response_id: response_id.clone(),
                },
                now_ms,
                max_events,
            )?;
            row.session.lock_state = LockState::Busy {
                response_id: response_id.clone(),
            };
            row.lock_seq = Some(seq);
            seq
        };
        self.notify.notify_waiters();
        Ok(seq)
    }

    async fn end_turn(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        response_id: &ResponseId,
        status: ResponseStatus,
        now_ms: u64,
    ) -> Result<u64, SessionError> {
        self.guard_writable()?;
        let max_events = self.store.max_events_per_session();
        let seq = {
            let mut g = self.store.lock();
            let row = Self::row_mut(&mut g, tenant, id)?;

            // Idempotent: an execution-side retry must not append a second
            // terminal event, and must still learn the sequence number.
            if let Some((completed, seq)) = &row.last_completed {
                if completed == response_id {
                    return Ok(*seq);
                }
            }

            let seq = append_locked(
                row,
                SessionEventKind::TurnCompleted {
                    response_id: response_id.clone(),
                    status,
                },
                now_ms,
                max_events,
            )?;
            // Release only if this response is the holder. A late terminal from a
            // superseded attempt must not unlock a turn that has since started.
            if row.session.lock_state.holder() == Some(response_id) {
                row.session.lock_state = LockState::Idle;
                row.lock_seq = None;
            }
            row.last_completed = Some((response_id.clone(), seq));
            seq
        };
        self.notify.notify_waiters();
        Ok(seq)
    }

    async fn release_stale_lock(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        holder: &ResponseId,
    ) -> Result<bool, SessionError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        let row = Self::row_mut(&mut g, tenant, id)?;
        // Conditional on `holder` still holding it, so racing with a legitimate
        // new turn cannot unlock that turn.
        if row.session.lock_state.holder() != Some(holder) {
            return Ok(false);
        }
        row.session.lock_state = LockState::Idle;
        row.lock_seq = None;
        // No event, and therefore no notify: the terminal event was already
        // emitted by whoever completed the response.
        Ok(true)
    }

    async fn append_event(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        kind: SessionEventKind,
        now_ms: u64,
    ) -> Result<u64, SessionError> {
        self.guard_writable()?;
        let max_events = self.store.max_events_per_session();
        let seq = {
            let mut g = self.store.lock();
            let row = Self::row_mut(&mut g, tenant, id)?;
            append_locked(row, kind, now_ms, max_events)?
        };
        self.notify.notify_waiters();
        Ok(seq)
    }

    async fn read_after(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        starting_after: Option<u64>,
        limit: usize,
        wait_ms: u64,
    ) -> Result<Vec<SessionEvent>, SessionError> {
        self.guard_available()?;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms);
        loop {
            {
                let g = self.store.lock();
                let row = match g.sessions.get(id) {
                    None => return Err(SessionError::NotFound),
                    Some(row) if &row.session.tenant_id != tenant => {
                        return Err(SessionError::NotFound)
                    }
                    Some(row) => row,
                };
                // The cursor is exclusive, and `None` means "from the beginning"
                // because 0 is itself a valid sequence number (INV-11).
                let target = match starting_after {
                    None => 0usize,
                    Some(after) => after.saturating_add(1) as usize,
                };
                if target < row.events.len() {
                    // Direct index: contiguity guarantees position. Nothing is
                    // ever evicted here, so there is no expired-cursor case.
                    return Ok(row.events[target..]
                        .iter()
                        .take(limit)
                        .cloned()
                        .collect());
                }
            }
            if tokio::time::Instant::now() >= deadline {
                // Empty and still connected: a subscriber that arrives before any
                // event is not an error (see the transcript/subscribe scenarios).
                return Ok(vec![]);
            }
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {}
            }
        }
    }

    async fn health(&self) -> Result<(), SessionError> {
        self.guard_available()
    }

    fn set_max_events_per_session(&self, limit: usize) {
        self.store.set_max_events_per_session(limit);
    }
}
