//! PostgreSQL conversation store (D27).
//!
//! A conversation is a pointer to the tail of a response chain, so this is a
//! single narrow table with no child rows, no cursor paging and no hot read path.
//! Context assembly never comes through here: it resolves the chain from the
//! materialised snapshot (D24), reached via `last_response_id`.

use std::collections::BTreeMap;

use async_trait::async_trait;
use nova_responses_core::{
    Conversation, ConversationError, ConversationId, ConversationStore, ResponseId, TenantId,
};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use crate::error::to_conversation_error;

/// Columns every full-record query must select.
const COLUMNS: &str =
    "conversation_id, tenant_id, last_response_id, metadata, created_at_ms";

pub struct SqlConversationStore {
    pool: PgPool,
}

impl SqlConversationStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn conversation_from_row(row: &PgRow) -> Result<Conversation, ConversationError> {
    let decode = |what: &str, e: String| ConversationError::Internal(format!("{what}: {e}"));

    let id: String = row.try_get("conversation_id").map_err(to_conversation_error)?;
    let tenant_id: String = row.try_get("tenant_id").map_err(to_conversation_error)?;
    let last: Option<String> = row
        .try_get("last_response_id")
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
                 (conversation_id, tenant_id, last_response_id, metadata, created_at_ms) \
             VALUES ($1,$2,$3,$4,$5)",
        )
        .bind(conversation.id.to_string())
        .bind(conversation.tenant_id.as_str())
        .bind(conversation.last_response_id.as_ref().map(|v| v.to_string()))
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

    async fn health(&self) -> Result<(), ConversationError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(to_conversation_error)?;
        Ok(())
    }
}
