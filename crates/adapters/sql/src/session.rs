//! PostgreSQL session store (D26).
//!
//! Every mutation runs inside one transaction that begins by taking a row lock on
//! `sessions` (`SELECT … FOR UPDATE`). That single pattern buys three things at
//! once, which is why it is used even where a bare `UPDATE` would appear to
//! suffice:
//!
//! 1. **The turn lock and its event commit together.** A crash between them would
//!    leave a session locked with nothing on the stream to explain it, or an
//!    announced turn no lock is holding — permanent disagreement between every
//!    device and the server.
//! 2. **Sequence numbers stay contiguous under concurrency.** `next_seq` is read
//!    and bumped while the row is held, so two concurrent appends serialise
//!    instead of colliding. The composite primary key on `session_events` is the
//!    backstop that turns any residual race into an error rather than a
//!    duplicate.
//! 3. **Tenant scoping happens once.** The locking read carries the tenant
//!    predicate, so a foreign session is indistinguishable from a missing one
//!    (SEC-2).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use nova_responses_core::{
    ConversationId, LockState, ResponseId, ResponseStatus, Session, SessionError, SessionEvent,
    SessionEventKind, SessionId, SessionStore, TenantId,
};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Postgres, Row, Transaction};

use crate::error::{is_unique_violation, to_session_error};

/// How long to sleep between polls while long-polling `read_after`.
///
/// Postgres has no equivalent of the in-memory notifier, so waiting means
/// re-querying. 50 ms keeps added latency well below the perceptual threshold for
/// a turn boundary while capping the query rate per waiting subscriber — the
/// token-level stream, which is the latency-critical one, does not come through
/// here at all.
const POLL_INTERVAL_MS: u64 = 50;

pub struct SqlSessionStore {
    pool: PgPool,
    /// Upper bound on one session's event stream.
    ///
    /// Reaching it **refuses the append** rather than evicting the oldest events,
    /// unlike the per-response event ring: dropping a turn boundary or a business
    /// event would lose the only record that it happened. Atomic so the bound is
    /// changeable at runtime, mirroring the ledger's `set_pending_limit`.
    max_events_per_session: AtomicUsize,
}

const SESSION_COLUMNS: &str = "session_id, tenant_id, conversation_id, lock_response_id, \
     lock_seq, last_completed_response_id, last_completed_seq, next_seq, created_at_ms";

impl SqlSessionStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            max_events_per_session: AtomicUsize::new(100_000),
        }
    }

    pub fn with_max_events_per_session(pool: PgPool, max_events_per_session: usize) -> Self {
        Self {
            pool,
            max_events_per_session: AtomicUsize::new(max_events_per_session.max(1)),
        }
    }

    fn max_events_per_session(&self) -> usize {
        self.max_events_per_session.load(Ordering::SeqCst)
    }

    /// Lock the session row for the rest of the transaction, or report it absent.
    async fn lock_row(
        tx: &mut Transaction<'_, Postgres>,
        tenant: &TenantId,
        id: &SessionId,
    ) -> Result<PgRow, SessionError> {
        sqlx::query(&format!(
            "SELECT {SESSION_COLUMNS} FROM sessions \
             WHERE session_id = $1 AND tenant_id = $2 FOR UPDATE"
        ))
        .bind(id.to_string())
        .bind(tenant.as_str())
        .fetch_optional(&mut **tx)
        .await
        .map_err(to_session_error)?
        .ok_or(SessionError::NotFound)
    }

    /// Append `kind` using the locked row's `next_seq`, and advance the counter.
    ///
    /// The only place a sequence number is minted, so contiguity cannot be broken
    /// by one call site forgetting to bump the counter.
    async fn append_locked(
        tx: &mut Transaction<'_, Postgres>,
        id: &SessionId,
        next_seq: i64,
        kind: &SessionEventKind,
        now_ms: u64,
        max_events: usize,
    ) -> Result<u64, SessionError> {
        if next_seq as usize >= max_events {
            return Err(SessionError::CapacityExceeded);
        }
        let payload = serde_json::to_value(kind)
            .map_err(|e| SessionError::Internal(format!("event kind: {e}")))?;

        sqlx::query(
            "INSERT INTO session_events (session_id, seq, kind, ts_ms) VALUES ($1,$2,$3,$4)",
        )
        .bind(id.to_string())
        .bind(next_seq)
        .bind(payload)
        .bind(now_ms as i64)
        .execute(&mut **tx)
        .await
        .map_err(to_session_error)?;

        sqlx::query("UPDATE sessions SET next_seq = $2 WHERE session_id = $1")
            .bind(id.to_string())
            .bind(next_seq + 1)
            .execute(&mut **tx)
            .await
            .map_err(to_session_error)?;

        Ok(next_seq as u64)
    }
}

fn optional_response_id(
    raw: Option<String>,
    column: &str,
) -> Result<Option<ResponseId>, SessionError> {
    match raw {
        None => Ok(None),
        Some(raw) => ResponseId::parse(&raw)
            .map(Some)
            .map_err(|e| SessionError::Internal(format!("{column}: {e}"))),
    }
}

fn session_from_row(row: &PgRow) -> Result<Session, SessionError> {
    let id: String = row.try_get("session_id").map_err(to_session_error)?;
    let tenant_id: String = row.try_get("tenant_id").map_err(to_session_error)?;
    let conversation_id: String = row.try_get("conversation_id").map_err(to_session_error)?;
    let lock: Option<String> = row.try_get("lock_response_id").map_err(to_session_error)?;
    let created_at_ms: i64 = row.try_get("created_at_ms").map_err(to_session_error)?;

    let decode = |what: &str, e: String| SessionError::Internal(format!("{what}: {e}"));

    Ok(Session {
        id: SessionId::parse(&id).map_err(|e| decode("session_id", e.to_string()))?,
        tenant_id: TenantId::parse(&tenant_id)
            .map_err(|e| decode("tenant_id", e.to_string()))?,
        conversation_id: ConversationId::parse(&conversation_id)
            .map_err(|e| decode("conversation_id", e.to_string()))?,
        // Derived rather than stored as an enum column: one nullable id cannot
        // disagree with itself, whereas a state column beside it could.
        lock_state: match optional_response_id(lock, "lock_response_id")? {
            None => LockState::Idle,
            Some(response_id) => LockState::Busy { response_id },
        },
        created_at_ms: created_at_ms as u64,
    })
}

fn event_from_row(row: &PgRow) -> Result<SessionEvent, SessionError> {
    let session_id: String = row.try_get("session_id").map_err(to_session_error)?;
    let seq: i64 = row.try_get("seq").map_err(to_session_error)?;
    let kind: serde_json::Value = row.try_get("kind").map_err(to_session_error)?;
    let ts_ms: i64 = row.try_get("ts_ms").map_err(to_session_error)?;

    Ok(SessionEvent {
        session_id: SessionId::parse(&session_id)
            .map_err(|e| SessionError::Internal(format!("session_id: {e}")))?,
        seq: seq as u64,
        kind: serde_json::from_value(kind)
            .map_err(|e| SessionError::Internal(format!("event kind: {e}")))?,
        ts_ms: ts_ms as u64,
    })
}

#[async_trait]
impl SessionStore for SqlSessionStore {
    async fn create(&self, session: Session, now_ms: u64) -> Result<Session, SessionError> {
        let mut tx = self.pool.begin().await.map_err(to_session_error)?;

        sqlx::query(
            "INSERT INTO sessions (session_id, tenant_id, conversation_id, next_seq, created_at_ms) \
             VALUES ($1,$2,$3,0,$4)",
        )
        .bind(session.id.to_string())
        .bind(session.tenant_id.as_str())
        .bind(session.conversation_id.to_string())
        .bind(session.created_at_ms as i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            // The unique index on (tenant_id, conversation_id) is what enforces
            // exclusive binding, so its violation is a domain answer rather than
            // an internal error.
            if is_unique_violation(&e) {
                SessionError::ConversationTaken
            } else {
                to_session_error(e)
            }
        })?;

        // Sequence 0 exists from the start, so a subscriber can tell "stream
        // begins here" from "nothing has happened yet". In the same transaction as
        // the insert: a session with no stream would be unreadable.
        Self::append_locked(
            &mut tx,
            &session.id,
            0,
            &SessionEventKind::SessionCreated,
            now_ms,
            self.max_events_per_session(),
        )
        .await?;

        tx.commit().await.map_err(to_session_error)?;
        Ok(session)
    }

    async fn get(
        &self,
        tenant: &TenantId,
        id: &SessionId,
    ) -> Result<Option<Session>, SessionError> {
        let row = sqlx::query(&format!(
            "SELECT {SESSION_COLUMNS} FROM sessions \
             WHERE session_id = $1 AND tenant_id = $2"
        ))
        .bind(id.to_string())
        .bind(tenant.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(to_session_error)?;

        row.as_ref().map(session_from_row).transpose()
    }

    async fn get_by_conversation(
        &self,
        tenant: &TenantId,
        conversation: &ConversationId,
    ) -> Result<Option<Session>, SessionError> {
        // Served by the unique index, which is also what guarantees at most one
        // row can match.
        let row = sqlx::query(&format!(
            "SELECT {SESSION_COLUMNS} FROM sessions \
             WHERE tenant_id = $1 AND conversation_id = $2"
        ))
        .bind(tenant.as_str())
        .bind(conversation.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(to_session_error)?;

        row.as_ref().map(session_from_row).transpose()
    }

    async fn delete(&self, tenant: &TenantId, id: &SessionId) -> Result<bool, SessionError> {
        // `session_events` goes with it through `ON DELETE CASCADE`.
        let result = sqlx::query("DELETE FROM sessions WHERE session_id = $1 AND tenant_id = $2")
            .bind(id.to_string())
            .bind(tenant.as_str())
            .execute(&self.pool)
            .await
            .map_err(to_session_error)?;
        Ok(result.rows_affected() > 0)
    }

    async fn delete_by_tenant(&self, tenant: &TenantId) -> Result<u64, SessionError> {
        let result = sqlx::query("DELETE FROM sessions WHERE tenant_id = $1")
            .bind(tenant.as_str())
            .execute(&self.pool)
            .await
            .map_err(to_session_error)?;
        Ok(result.rows_affected())
    }

    async fn begin_turn(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        response_id: &ResponseId,
        now_ms: u64,
    ) -> Result<u64, SessionError> {
        let mut tx = self.pool.begin().await.map_err(to_session_error)?;
        let row = Self::lock_row(&mut tx, tenant, id).await?;

        let holder = optional_response_id(
            row.try_get("lock_response_id").map_err(to_session_error)?,
            "lock_response_id",
        )?;
        if let Some(holder) = holder {
            // Re-entry by the holder returns the sequence already assigned rather
            // than announcing the same turn twice.
            if &holder == response_id {
                let lock_seq: Option<i64> = row.try_get("lock_seq").map_err(to_session_error)?;
                return lock_seq.map(|s| s as u64).ok_or_else(|| {
                    SessionError::Internal(
                        "session is busy but no sequence was recorded for the turn".into(),
                    )
                });
            }
            // Nothing has been written, and the transaction is dropped without
            // committing: a refused turn leaves no trace.
            return Err(SessionError::Busy { holder });
        }

        let next_seq: i64 = row.try_get("next_seq").map_err(to_session_error)?;
        let seq = Self::append_locked(
            &mut tx,
            id,
            next_seq,
            &SessionEventKind::TurnStarted {
                response_id: response_id.clone(),
            },
            now_ms,
            self.max_events_per_session(),
        )
        .await?;

        sqlx::query(
            "UPDATE sessions SET lock_response_id = $2, lock_seq = $3 WHERE session_id = $1",
        )
        .bind(id.to_string())
        .bind(response_id.to_string())
        .bind(seq as i64)
        .execute(&mut *tx)
        .await
        .map_err(to_session_error)?;

        tx.commit().await.map_err(to_session_error)?;
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
        let mut tx = self.pool.begin().await.map_err(to_session_error)?;
        let row = Self::lock_row(&mut tx, tenant, id).await?;

        // Idempotent: an execution-side retry must not append a second terminal
        // event, and must still learn the sequence number.
        let last_completed = optional_response_id(
            row.try_get("last_completed_response_id")
                .map_err(to_session_error)?,
            "last_completed_response_id",
        )?;
        if last_completed.as_ref() == Some(response_id) {
            let seq: Option<i64> = row
                .try_get("last_completed_seq")
                .map_err(to_session_error)?;
            if let Some(seq) = seq {
                return Ok(seq as u64);
            }
        }

        let next_seq: i64 = row.try_get("next_seq").map_err(to_session_error)?;
        let seq = Self::append_locked(
            &mut tx,
            id,
            next_seq,
            &SessionEventKind::TurnCompleted {
                response_id: response_id.clone(),
                status,
            },
            now_ms,
            self.max_events_per_session(),
        )
        .await?;

        // Release only if this response is the holder. A late terminal from a
        // superseded attempt must not unlock a turn that has since started, which
        // is what the `lock_response_id = $3` predicate enforces.
        sqlx::query(
            "UPDATE sessions SET \
                 lock_response_id = CASE WHEN lock_response_id = $3 THEN NULL \
                                         ELSE lock_response_id END, \
                 lock_seq = CASE WHEN lock_response_id = $3 THEN NULL ELSE lock_seq END, \
                 last_completed_response_id = $3, \
                 last_completed_seq = $2 \
             WHERE session_id = $1",
        )
        .bind(id.to_string())
        .bind(seq as i64)
        .bind(response_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(to_session_error)?;

        tx.commit().await.map_err(to_session_error)?;
        Ok(seq)
    }

    async fn release_stale_lock(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        holder: &ResponseId,
    ) -> Result<bool, SessionError> {
        // Single conditional statement, no transaction needed: the
        // `lock_response_id = $3` predicate is the compare-and-set, so racing
        // with a legitimate new turn cannot unlock that turn. No event is written
        // — the terminal event was already emitted by whoever completed the
        // response.
        let result = sqlx::query(
            "UPDATE sessions SET lock_response_id = NULL, lock_seq = NULL \
             WHERE session_id = $1 AND tenant_id = $2 AND lock_response_id = $3",
        )
        .bind(id.to_string())
        .bind(tenant.as_str())
        .bind(holder.to_string())
        .execute(&self.pool)
        .await
        .map_err(to_session_error)?;
        Ok(result.rows_affected() > 0)
    }

    async fn append_event(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        kind: SessionEventKind,
        now_ms: u64,
    ) -> Result<u64, SessionError> {
        let mut tx = self.pool.begin().await.map_err(to_session_error)?;
        let row = Self::lock_row(&mut tx, tenant, id).await?;
        let next_seq: i64 = row.try_get("next_seq").map_err(to_session_error)?;
        let seq = Self::append_locked(
            &mut tx,
            id,
            next_seq,
            &kind,
            now_ms,
            self.max_events_per_session(),
        )
        .await?;
        tx.commit().await.map_err(to_session_error)?;
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
        let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms);
        // The cursor is exclusive, and `None` means "from the beginning" because 0
        // is itself a valid sequence number (INV-11).
        let after: i64 = match starting_after {
            None => -1,
            Some(after) => after as i64,
        };

        loop {
            // Existence is checked as part of the read so a deleted session is
            // reported rather than long-polled forever.
            let exists: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM sessions WHERE session_id = $1 AND tenant_id = $2",
            )
            .bind(id.to_string())
            .bind(tenant.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(to_session_error)?;
            if exists.is_none() {
                return Err(SessionError::NotFound);
            }

            // Served straight from the `(session_id, seq)` primary key: a range
            // scan in sequence order, never an OFFSET walk.
            let rows = sqlx::query(
                "SELECT session_id, seq, kind, ts_ms FROM session_events \
                 WHERE session_id = $1 AND seq > $2 ORDER BY seq LIMIT $3",
            )
            .bind(id.to_string())
            .bind(after)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(to_session_error)?;

            if !rows.is_empty() {
                return rows.iter().map(event_from_row).collect();
            }
            if tokio::time::Instant::now() >= deadline {
                // Empty and still connected: a subscriber that arrives before any
                // event is not an error.
                return Ok(vec![]);
            }
            let remaining = deadline - tokio::time::Instant::now();
            tokio::time::sleep(remaining.min(Duration::from_millis(POLL_INTERVAL_MS))).await;
        }
    }

    async fn health(&self) -> Result<(), SessionError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(to_session_error)?;
        Ok(())
    }

    fn set_max_events_per_session(&self, limit: usize) {
        self.max_events_per_session
            .store(limit.max(1), Ordering::SeqCst);
    }
}
