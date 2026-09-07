//! PostgreSQL conversation store (D27).
//!
//! A conversation is a pointer to the tail of a response chain, so this is a
//! single narrow table with no child rows, no cursor paging and no hot read path.
//! Context assembly never comes through here: it resolves the chain from the
//! materialised snapshot (D24), reached via `last_response_id`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use nova_responses_core::{
    Conversation, ConversationError, ConversationEvent, ConversationEventKind, ConversationId,
    ConversationStore, ResponseId, ResponseStatus, TenantId,
};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Postgres, Row, Transaction};

use crate::error::to_conversation_error;

/// How long to sleep between polls while long-polling `read_after`.
const POLL_INTERVAL_MS: u64 = 50;

/// Columns every full-record query must select.
const COLUMNS: &str =
    "conversation_id, tenant_id, last_response_id, active_response_id, next_seq, metadata, created_at_ms";

pub struct SqlConversationStore {
    pool: PgPool,
    /// Upper bound on one conversation's event stream (D28).
    max_events_per_conversation: AtomicUsize,
}

impl SqlConversationStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            max_events_per_conversation: AtomicUsize::new(100_000),
        }
    }

    fn max_events_per_conversation(&self) -> usize {
        self.max_events_per_conversation.load(Ordering::SeqCst)
    }

    /// Lock the conversation row for the rest of the transaction, or report absent.
    async fn lock_row(
        tx: &mut Transaction<'_, Postgres>,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<PgRow, ConversationError> {
        sqlx::query(&format!(
            "SELECT {COLUMNS} FROM conversations \
             WHERE conversation_id = $1 AND tenant_id = $2 FOR UPDATE"
        ))
        .bind(id.to_string())
        .bind(tenant.as_str())
        .fetch_optional(&mut **tx)
        .await
        .map_err(to_conversation_error)?
        .ok_or(ConversationError::NotFound)
    }

    /// Append `kind` using the locked row's `next_seq`, then bump the counter.
    async fn append_locked(
        tx: &mut Transaction<'_, Postgres>,
        id: &ConversationId,
        next_seq: i64,
        kind: &ConversationEventKind,
        now_ms: u64,
        max_events: usize,
    ) -> Result<u64, ConversationError> {
        if next_seq as usize >= max_events {
            return Err(ConversationError::CapacityExceeded);
        }
        let payload = serde_json::to_value(kind)
            .map_err(|e| ConversationError::Internal(format!("event kind: {e}")))?;

        sqlx::query(
            "INSERT INTO conversation_events (conversation_id, seq, kind, ts_ms) \
             VALUES ($1,$2,$3,$4)",
        )
        .bind(id.to_string())
        .bind(next_seq)
        .bind(payload)
        .bind(now_ms as i64)
        .execute(&mut **tx)
        .await
        .map_err(to_conversation_error)?;

        sqlx::query("UPDATE conversations SET next_seq = $2 WHERE conversation_id = $1")
            .bind(id.to_string())
            .bind(next_seq + 1)
            .execute(&mut **tx)
            .await
            .map_err(to_conversation_error)?;

        Ok(next_seq as u64)
    }
}

fn conversation_from_row(row: &PgRow) -> Result<Conversation, ConversationError> {
    let decode = |what: &str, e: String| ConversationError::Internal(format!("{what}: {e}"));

    let id: String = row.try_get("conversation_id").map_err(to_conversation_error)?;
    let tenant_id: String = row.try_get("tenant_id").map_err(to_conversation_error)?;
    let last: Option<String> = row
        .try_get("last_response_id")
        .map_err(to_conversation_error)?;
    let active: Option<String> = row
        .try_get("active_response_id")
        .map_err(to_conversation_error)?;
    let metadata: serde_json::Value =
        row.try_get("metadata").map_err(to_conversation_error)?;
    let created_at_ms: i64 = row.try_get("created_at_ms").map_err(to_conversation_error)?;

    Ok(Conversation {
        id: ConversationId::parse(&id)
            .map_err(|e| decode("conversation_id", e.to_string()))?,
        tenant_id: TenantId::parse(&tenant_id)
            .map_err(|e| decode("tenant_id", e.to_string()))?,
        last_response_id: match last {
            None => None,
            Some(raw) => Some(
                ResponseId::parse(&raw)
                    .map_err(|e| decode("last_response_id", e.to_string()))?,
            ),
        },
        active_response_id: match active {
            None => None,
            Some(raw) => Some(
                ResponseId::parse(&raw)
                    .map_err(|e| decode("active_response_id", e.to_string()))?,
            ),
        },
        metadata: serde_json::from_value(metadata)
            .map_err(|e| decode("metadata", e.to_string()))?,
        created_at_ms: created_at_ms as u64,
    })
}

fn metadata_to_json(metadata: &BTreeMap<String, String>) -> serde_json::Value {
    serde_json::to_value(metadata).unwrap_or_else(|_| serde_json::Value::Object(Default::default()))
}

#[async_trait]
impl ConversationStore for SqlConversationStore {
    async fn create(
        &self,
        conversation: Conversation,
    ) -> Result<Conversation, ConversationError> {
        sqlx::query(
            "INSERT INTO conversations \
                 (conversation_id, tenant_id, last_response_id, active_response_id, metadata, created_at_ms) \
             VALUES ($1,$2,$3,$4,$5,$6)",
        )
        .bind(conversation.id.to_string())
        .bind(conversation.tenant_id.as_str())
        .bind(conversation.last_response_id.as_ref().map(|v| v.to_string()))
        .bind(conversation.active_response_id.as_ref().map(|v| v.to_string()))
        .bind(metadata_to_json(&conversation.metadata))
        .bind(conversation.created_at_ms as i64)
        .execute(&self.pool)
        .await
        .map_err(to_conversation_error)?;
        Ok(conversation)
    }

    async fn get(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<Option<Conversation>, ConversationError> {
        // The tenant predicate is in the query, so a foreign row is indisting-
        // uishable from a missing one without a second round trip that could
        // reveal existence (SEC-2).
        let row = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM conversations \
             WHERE conversation_id = $1 AND tenant_id = $2"
        ))
        .bind(id.to_string())
        .bind(tenant.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(to_conversation_error)?;

        row.as_ref().map(conversation_from_row).transpose()
    }

    async fn update_metadata(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        metadata: BTreeMap<String, String>,
    ) -> Result<Conversation, ConversationError> {
        // `RETURNING` makes this one round trip and removes the window in which a
        // separate read-back could observe someone else's write.
        let row = sqlx::query(&format!(
            "UPDATE conversations SET metadata = $3 \
             WHERE conversation_id = $1 AND tenant_id = $2 \
             RETURNING {COLUMNS}"
        ))
        .bind(id.to_string())
        .bind(tenant.as_str())
        .bind(metadata_to_json(&metadata))
        .fetch_optional(&self.pool)
        .await
        .map_err(to_conversation_error)?;

        match row {
            None => Err(ConversationError::NotFound),
            Some(row) => conversation_from_row(&row),
        }
    }

    async fn delete(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<bool, ConversationError> {
        // Response rows are untouched: deletion does not cascade, matching D24 and
        // upstream's own wording.
        let result = sqlx::query(
            "DELETE FROM conversations WHERE conversation_id = $1 AND tenant_id = $2",
        )
        .bind(id.to_string())
        .bind(tenant.as_str())
        .execute(&self.pool)
        .await
        .map_err(to_conversation_error)?;
        Ok(result.rows_affected() > 0)
    }

    async fn delete_by_tenant(&self, tenant: &TenantId) -> Result<u64, ConversationError> {
        let result = sqlx::query("DELETE FROM conversations WHERE tenant_id = $1")
            .bind(tenant.as_str())
            .execute(&self.pool)
            .await
            .map_err(to_conversation_error)?;
        Ok(result.rows_affected())
    }

    async fn advance(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        last: &ResponseId,
    ) -> Result<(), ConversationError> {
        // Unconditional assignment: last write wins by design, so there is no
        // `AND last_response_id = $expected` predicate here. See the port
        // documentation for why a compare-and-set would be wrong.
        let result = sqlx::query(
            "UPDATE conversations SET last_response_id = $3 \
             WHERE conversation_id = $1 AND tenant_id = $2",
        )
        .bind(id.to_string())
        .bind(tenant.as_str())
        .bind(last.to_string())
        .execute(&self.pool)
        .await
        .map_err(to_conversation_error)?;

        if result.rows_affected() == 0 {
            return Err(ConversationError::NotFound);
        }
        Ok(())
    }

    async fn acquire_active(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        response_id: &ResponseId,
        now_ms: u64,
    ) -> Result<u64, ConversationError> {
        let mut tx = self.pool.begin().await.map_err(to_conversation_error)?;
        let row = Self::lock_row(&mut tx, tenant, id).await?;

        let holder: Option<String> = row
            .try_get("active_response_id")
            .map_err(to_conversation_error)?;
        if let Some(holder) = holder {
            if holder == response_id.to_string() {
                // Re-entrant: the turn was already announced, so no second event.
                return Ok(0);
            }
            // Nothing written, transaction dropped uncommitted: a refused turn
            // leaves no trace.
            return Err(ConversationError::Busy {
                holder: ResponseId::parse(&holder)
                    .map_err(|e| ConversationError::Internal(e.to_string()))?,
            });
        }

        let next_seq: i64 = row.try_get("next_seq").map_err(to_conversation_error)?;
        let seq = Self::append_locked(
            &mut tx,
            id,
            next_seq,
            &ConversationEventKind::TurnStarted {
                response_id: response_id.clone(),
            },
            now_ms,
            self.max_events_per_conversation(),
        )
        .await?;

        sqlx::query(
            "UPDATE conversations SET active_response_id = $3 \
             WHERE conversation_id = $1 AND tenant_id = $2",
        )
        .bind(id.to_string())
        .bind(tenant.as_str())
        .bind(response_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(to_conversation_error)?;

        tx.commit().await.map_err(to_conversation_error)?;
        Ok(seq)
    }

    async fn release_active(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        response_id: &ResponseId,
        status: ResponseStatus,
        now_ms: u64,
    ) -> Result<u64, ConversationError> {
        let mut tx = self.pool.begin().await.map_err(to_conversation_error)?;
        let row = Self::lock_row(&mut tx, tenant, id).await?;

        let holder: Option<String> = row
            .try_get("active_response_id")
            .map_err(to_conversation_error)?;
        // Conditional release: only release if we still hold it. A raced newer
        // turn is left untouched, and re-entry is a no-op.
        if holder != Some(response_id.to_string()) {
            return Ok(0);
        }

        let next_seq: i64 = row.try_get("next_seq").map_err(to_conversation_error)?;
        let seq = Self::append_locked(
            &mut tx,
            id,
            next_seq,
            &ConversationEventKind::TurnCompleted {
                response_id: response_id.clone(),
                status,
            },
            now_ms,
            self.max_events_per_conversation(),
        )
        .await?;

        sqlx::query(
            "UPDATE conversations SET active_response_id = NULL \
             WHERE conversation_id = $1 AND tenant_id = $2 AND active_response_id = $3",
        )
        .bind(id.to_string())
        .bind(tenant.as_str())
        .bind(response_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(to_conversation_error)?;

        tx.commit().await.map_err(to_conversation_error)?;
        Ok(seq)
    }

    async fn release_stale_active(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        holder: &ResponseId,
    ) -> Result<bool, ConversationError> {
        // Single conditional statement: the `active_response_id = $3` predicate is
        // the compare-and-set, so racing with a legitimate new turn cannot unlock
        // it. No event is written — the terminal event was already emitted.
        let result = sqlx::query(
            "UPDATE conversations SET active_response_id = NULL \
             WHERE conversation_id = $1 AND tenant_id = $2 AND active_response_id = $3",
        )
        .bind(id.to_string())
        .bind(tenant.as_str())
        .bind(holder.to_string())
        .execute(&self.pool)
        .await
        .map_err(to_conversation_error)?;
        Ok(result.rows_affected() > 0)
    }

    async fn append_event(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        kind: ConversationEventKind,
        now_ms: u64,
    ) -> Result<u64, ConversationError> {
        let mut tx = self.pool.begin().await.map_err(to_conversation_error)?;
        let row = Self::lock_row(&mut tx, tenant, id).await?;
        let next_seq: i64 = row.try_get("next_seq").map_err(to_conversation_error)?;
        let seq = Self::append_locked(
            &mut tx,
            id,
            next_seq,
            &kind,
            now_ms,
            self.max_events_per_conversation(),
        )
        .await?;
        tx.commit().await.map_err(to_conversation_error)?;
        Ok(seq)
    }

    async fn read_after(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        starting_after: Option<u64>,
        limit: usize,
        wait_ms: u64,
    ) -> Result<Vec<ConversationEvent>, ConversationError> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms);
        let after: i64 = match starting_after {
            None => -1,
            Some(after) => after as i64,
        };

        loop {
            let exists: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM conversations WHERE conversation_id = $1 AND tenant_id = $2",
            )
            .bind(id.to_string())
            .bind(tenant.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(to_conversation_error)?;
            if exists.is_none() {
                return Err(ConversationError::NotFound);
            }

            let rows = sqlx::query(
                "SELECT conversation_id, seq, kind, ts_ms FROM conversation_events \
                 WHERE conversation_id = $1 AND seq > $2 ORDER BY seq LIMIT $3",
            )
            .bind(id.to_string())
            .bind(after)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(to_conversation_error)?;

            if !rows.is_empty() {
                return rows.iter().map(conversation_event_from_row).collect();
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(vec![]);
            }
            let remaining = deadline - tokio::time::Instant::now();
            tokio::time::sleep(remaining.min(Duration::from_millis(POLL_INTERVAL_MS))).await;
        }
    }

    async fn list(&self, tenant: &TenantId) -> Result<Vec<Conversation>, ConversationError> {
        let rows = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM conversations \
             WHERE tenant_id = $1 ORDER BY created_at_ms DESC, conversation_id"
        ))
        .bind(tenant.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(to_conversation_error)?;

        rows.iter().map(conversation_from_row).collect()
    }

    fn set_max_events_per_conversation(&self, limit: usize) {
        self.max_events_per_conversation
            .store(limit.max(1), Ordering::SeqCst);
    }

    async fn health(&self) -> Result<(), ConversationError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(to_conversation_error)?;
        Ok(())
    }
}

fn conversation_event_from_row(row: &PgRow) -> Result<ConversationEvent, ConversationError> {
    let conversation_id: String = row
        .try_get("conversation_id")
        .map_err(to_conversation_error)?;
    let seq: i64 = row.try_get("seq").map_err(to_conversation_error)?;
    let kind: serde_json::Value = row.try_get("kind").map_err(to_conversation_error)?;
    let ts_ms: i64 = row.try_get("ts_ms").map_err(to_conversation_error)?;

    Ok(ConversationEvent {
        conversation_id: ConversationId::parse(&conversation_id)
            .map_err(|e| ConversationError::Internal(format!("conversation_id: {e}")))?,
        seq: seq as u64,
        kind: serde_json::from_value(kind)
            .map_err(|e| ConversationError::Internal(format!("event kind: {e}")))?,
        ts_ms: ts_ms as u64,
    })
}
