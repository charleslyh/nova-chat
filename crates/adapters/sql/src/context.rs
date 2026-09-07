//! PostgreSQL context store.
//!
//! History is materialised (D24): resolution is a single row read, not a walk.
//! The `WITH RECURSIVE` machinery an earlier revision required is gone — a
//! response carries its whole context, so resolution no longer traverses
//! `previous_response_id` at all.

use async_trait::async_trait;
use nova_responses_core::{
    canonical_items, ChainLimits, ContentIntegrity, ContextError, ContextStore, ResolvedContext,
    ResponseId, ResponseItem, ResponseStatus, StoredResponse, TenantId, Usage,
};
use sqlx::PgPool;
use std::sync::Arc;

use crate::error::to_context_error;
use crate::row::{
    items_to_json, reasoning_to_json, record_from_row, status_to_str, usage_to_json, RECORD_COLUMNS,
};

pub struct SqlContextStore {
    pool: PgPool,
    integrity: Option<Arc<dyn ContentIntegrity>>,
}

impl SqlContextStore {
    pub fn new(pool: PgPool, integrity: Option<Arc<dyn ContentIntegrity>>) -> Self {
        Self { pool, integrity }
    }

    fn signing_input(record: &StoredResponse) -> String {
        format!(
            "{}|{}",
            canonical_items(&record.input_items),
            canonical_items(&record.output_items)
        )
    }

    fn sign(&self, record: &mut StoredResponse) -> Result<(), ContextError> {
        if let Some(integrity) = &self.integrity {
            let tag = integrity
                .sign(&Self::signing_input(record))
                .map_err(|e| ContextError::Internal(e.to_string()))?;
            record.integrity = Some(tag);
            record.integrity_alg = Some(integrity.alg().to_string());
        }
        Ok(())
    }

    fn verify(&self, record: &StoredResponse) -> Result<(), ContextError> {
        let Some(integrity) = &self.integrity else {
            return Ok(());
        };
        let Some(tag) = &record.integrity else {
            return Ok(());
        };
        integrity
            .verify(&Self::signing_input(record), tag)
            .map_err(|_| ContextError::IntegrityMismatch)
    }
}

#[async_trait]
impl ContextStore for SqlContextStore {
    async fn put(&self, mut record: StoredResponse) -> Result<(), ContextError> {
        self.sign(&mut record)?;
        let sql = "INSERT INTO responses (\
                response_id, previous_response_id, tenant_id, model, status, stored, node_tag, \
                attempt, owner, idempotency_key, instructions, input_items, output_items, \
                reasoning, context, context_reasoning, \
                context_depth, usage, \
                integrity, integrity_alg, created_at_ms, completed_at_ms, expires_at_ms, \
                conversation_id, session_id) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25) \
             ON CONFLICT (response_id) DO UPDATE SET \
                status = EXCLUDED.status, \
                stored = EXCLUDED.stored, \
                attempt = EXCLUDED.attempt, \
                owner = EXCLUDED.owner, \
                instructions = EXCLUDED.instructions, \
                input_items = EXCLUDED.input_items, \
                output_items = EXCLUDED.output_items, \
                reasoning = EXCLUDED.reasoning, \
                context = EXCLUDED.context, \
                context_reasoning = EXCLUDED.context_reasoning, \
                context_depth = EXCLUDED.context_depth, \
                usage = EXCLUDED.usage, \
                integrity = EXCLUDED.integrity, \
                integrity_alg = EXCLUDED.integrity_alg, \
                completed_at_ms = EXCLUDED.completed_at_ms, \
                expires_at_ms = EXCLUDED.expires_at_ms, \
                conversation_id = EXCLUDED.conversation_id, \
                session_id = EXCLUDED.session_id";
        sqlx::query(sql)
            .bind(record.response_id.to_string())
            .bind(record.previous_response_id.as_ref().map(|v| v.to_string()))
            .bind(record.tenant_id.as_str())
            .bind(&record.model)
            .bind(status_to_str(record.status))
            .bind(record.stored)
            .bind(record.node_tag.as_str())
            .bind(record.attempt.0 as i64)
            .bind(record.owner.map(|v| v.to_string()))
            .bind(record.idempotency_key.as_ref().map(|k| k.0.clone()))
            .bind(record.instructions.as_deref())
            .bind(items_to_json(&record.input_items))
            .bind(items_to_json(&record.output_items))
            .bind(record.reasoning.as_deref())
            .bind(items_to_json(&record.context))
            .bind(reasoning_to_json(&record.context_reasoning))
            .bind(record.context_depth as i64)
            .bind(usage_to_json(&record.usage))
            .bind(record.integrity.as_deref())
            .bind(record.integrity_alg.as_deref())
            .bind(record.created_at_ms as i64)
            .bind(record.completed_at_ms.map(|v| v as i64))
            .bind(record.expires_at_ms.map(|v| v as i64))
            .bind(record.conversation_id.as_ref().map(|v| v.to_string()))
            .bind(record.session_id.as_ref().map(|v| v.to_string()))
            .execute(&self.pool)
            .await
            .map_err(to_context_error)?;
        Ok(())
    }

    async fn append_output(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
        items: Vec<ResponseItem>,
        reasoning: Option<String>,
        usage: Usage,
        status: ResponseStatus,
        now_ms: u64,
    ) -> Result<(), ContextError> {
        // Read the input side so the integrity tag covers both lists.
        let existing = self
            .get(tenant, response_id)
            .await?
            .ok_or(ContextError::NotFound)?;
        let mut updated = existing;
        // Supplied directly by the execution side; never replayed from events
        // (INV-48).
        updated.output_items = items;
        updated.reasoning = reasoning;
        updated.usage = usage;
        updated.status = status;
        updated.completed_at_ms = Some(now_ms);
        self.sign(&mut updated)?;

        let affected = sqlx::query(
            "UPDATE responses SET output_items = $1, reasoning = $2, usage = $3, status = $4, \
                completed_at_ms = $5, integrity = $6, integrity_alg = $7 \
             WHERE response_id = $8 AND tenant_id = $9",
        )
        .bind(items_to_json(&updated.output_items))
        .bind(updated.reasoning.as_deref())
        .bind(usage_to_json(&updated.usage))
        .bind(status_to_str(status))
        .bind(now_ms as i64)
        .bind(updated.integrity.as_deref())
        .bind(updated.integrity_alg.as_deref())
        .bind(response_id.to_string())
        .bind(tenant.as_str())
        .execute(&self.pool)
        .await
        .map_err(to_context_error)?
        .rows_affected();

        if affected == 0 {
            return Err(ContextError::NotFound);
        }
        Ok(())
    }

    async fn get(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
    ) -> Result<Option<StoredResponse>, ContextError> {
        let sql = format!(
            "SELECT {RECORD_COLUMNS} FROM responses WHERE response_id = $1 AND tenant_id = $2"
        );
        // Tenant is part of the predicate, so a cross-tenant read is
        // indistinguishable from a miss (SEC-2).
        let row = sqlx::query(&sql)
            .bind(response_id.to_string())
            .bind(tenant.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(to_context_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let record = record_from_row(&row).map_err(|e| ContextError::Internal(e.to_string()))?;
        self.verify(&record)?;
        Ok(Some(record))
    }

    async fn resolve_chain(
        &self,
        tenant: &TenantId,
        from: &ResponseId,
        limits: ChainLimits,
    ) -> Result<ResolvedContext, ContextError> {
        // History is materialised (D24): a single row read, not a recursive walk.
        // The `WITH RECURSIVE` machinery this used to require is gone — resolution
        // no longer depends on the continued existence of ancestors.
        let sql = format!(
            "SELECT {RECORD_COLUMNS} FROM responses WHERE response_id = $1 AND tenant_id = $2"
        );
        let row = sqlx::query(&sql)
            .bind(from.to_string())
            .bind(tenant.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(to_context_error)?;

        let Some(row) = row else {
            // The anchor is absent or foreign. Reported as a broken chain rather
            // than a miss: `previous_response_id` is a request field, not the
            // addressed resource — and this keeps mem and sql identical.
            return Err(ContextError::ChainBroken(from.to_string()));
        };

        let record = record_from_row(&row).map_err(|e| ContextError::Internal(e.to_string()))?;
        if !record.stored {
            return Err(ContextError::NotStored);
        }
        self.verify(&record)?;

        // Flat materialised history (D24): the ancestors' snapshot plus this
        // response's own items, with reasoning blocks aligned for rendering.
        let (items, reasoning) = record.resolved_items_and_reasoning();

        let depth = record.context_depth.saturating_add(1);
        if depth > limits.max_depth {
            return Err(ContextError::ChainTooLong {
                limit: limits.max_depth,
            });
        }
        if items.len() > limits.max_items {
            return Err(ContextError::ChainTooLong {
                limit: limits.max_items,
            });
        }
        let bytes: usize = items.iter().map(ResponseItem::byte_len).sum();
        if bytes > limits.max_bytes {
            return Err(ContextError::ChainTooLarge {
                limit: limits.max_bytes,
            });
        }

        Ok(ResolvedContext {
            items,
            reasoning,
            depth,
            bytes,
        })
    }

    async fn delete(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
    ) -> Result<bool, ContextError> {
        // Record-level deletion only (D24). Descendants carry their own flat copy
        // of the history, so removing this row does not touch them — "remove from
        // the conversation" does not mean "erase from every snapshot that inherited
        // it". No cascade, no transaction needed.
        let deleted = sqlx::query("DELETE FROM responses WHERE response_id = $1 AND tenant_id = $2")
            .bind(response_id.to_string())
            .bind(tenant.as_str())
            .execute(&self.pool)
            .await
            .map_err(to_context_error)?
            .rows_affected();

        Ok(deleted > 0)
    }

    async fn delete_by_tenant(&self, tenant: &TenantId) -> Result<u64, ContextError> {
        // Batched: a single unbounded DELETE over a large tenant would hold
        // locks and bloat WAL for the duration.
        const BATCH: i64 = 500;
        let mut total = 0u64;
        loop {
            let affected = sqlx::query(
                "DELETE FROM responses WHERE response_id IN ( \
                     SELECT response_id FROM responses WHERE tenant_id = $1 LIMIT $2 )",
            )
            .bind(tenant.as_str())
            .bind(BATCH)
            .execute(&self.pool)
            .await
            .map_err(to_context_error)?
            .rows_affected();
            total += affected;
            if affected < BATCH as u64 {
                break;
            }
        }
        Ok(total)
    }

    async fn sweep_expired(&self, now_ms: u64, limit: usize) -> Result<u64, ContextError> {
        let affected = sqlx::query(
            "DELETE FROM responses WHERE response_id IN ( \
                 SELECT response_id FROM responses \
                  WHERE expires_at_ms IS NOT NULL AND expires_at_ms <= $1 \
                  LIMIT $2 )",
        )
        .bind(now_ms as i64)
        .bind(limit as i64)
        .execute(&self.pool)
        .await
        .map_err(to_context_error)?
        .rows_affected();
        Ok(affected)
    }

    async fn health(&self) -> Result<(), ContextError> {
        sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map_err(to_context_error)?;
        Ok(())
    }
}
