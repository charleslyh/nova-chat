//! PostgreSQL context store.
//!
//! The reason this backend is a Postgres-family database rather than anything
//! else: `WITH RECURSIVE` collapses chain resolution from N round trips into a
//! single query. At depth 50 that is the difference between one network hop and
//! fifty on the critical path of every chained request.

use async_trait::async_trait;
use nova_responses_core::{
    canonical_items, ChainLimits, ContentIntegrity, ContextError, ContextStore, ResolvedContext,
    ResponseId, ResponseItem, ResponseStatus, StoredResponse, TenantId, Usage,
};
use sqlx::{PgPool, Row};
use std::sync::Arc;

use crate::error::to_context_error;
use crate::row::{
    items_from_json, items_to_json, record_from_row, status_to_str, usage_to_json, RECORD_COLUMNS,
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
    fn is_shared(&self) -> bool {
        // Shared storage: every node reads directly, and chain affinity routing
        // must therefore be disabled (D21).
        true
    }

    async fn put(&self, mut record: StoredResponse) -> Result<(), ContextError> {
        self.sign(&mut record)?;
        let sql = "INSERT INTO responses (\
                response_id, previous_response_id, tenant_id, model, status, stored, node_tag, \
                attempt, owner, idempotency_key, instructions, input_items, output_items, usage, \
                integrity, integrity_alg, created_at_ms, completed_at_ms, expires_at_ms) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19) \
             ON CONFLICT (response_id) DO UPDATE SET \
                status = EXCLUDED.status, \
                stored = EXCLUDED.stored, \
                attempt = EXCLUDED.attempt, \
                owner = EXCLUDED.owner, \
                instructions = EXCLUDED.instructions, \
                input_items = EXCLUDED.input_items, \
                output_items = EXCLUDED.output_items, \
                usage = EXCLUDED.usage, \
                integrity = EXCLUDED.integrity, \
                integrity_alg = EXCLUDED.integrity_alg, \
                completed_at_ms = EXCLUDED.completed_at_ms, \
                expires_at_ms = EXCLUDED.expires_at_ms";
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
            .bind(usage_to_json(&record.usage))
            .bind(record.integrity.as_deref())
            .bind(record.integrity_alg.as_deref())
            .bind(record.created_at_ms as i64)
            .bind(record.completed_at_ms.map(|v| v as i64))
            .bind(record.expires_at_ms.map(|v| v as i64))
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
        updated.usage = usage;
        updated.status = status;
        updated.completed_at_ms = Some(now_ms);
        self.sign(&mut updated)?;

        let affected = sqlx::query(
            "UPDATE responses SET output_items = $1, usage = $2, status = $3, \
                completed_at_ms = $4, integrity = $5, integrity_alg = $6 \
             WHERE response_id = $7 AND tenant_id = $8",
        )
        .bind(items_to_json(&updated.output_items))
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
        // Every value is a bound parameter — including the depth bound — so no
        // part of this statement is ever built by string concatenation (SEC-8).
        //
        // Note what the recursive term does *not* filter on: tenant and stored.
        // Filtering there would make a cross-tenant or unstored link look like
        // the chain simply ending, i.e. a silent truncation. Instead both are
        // selected and judged per hop below, which yields a precise error
        // (INV-42 / INV-43). `instructions` is not selected at all — it must
        // never reach chain output (INV-49).
        let sql = "WITH RECURSIVE chain AS ( \
                SELECT response_id, previous_response_id, tenant_id, stored, \
                       input_items, output_items, 1 AS depth \
                  FROM responses \
                 WHERE response_id = $1 AND tenant_id = $2 \
                UNION ALL \
                SELECT r.response_id, r.previous_response_id, r.tenant_id, r.stored, \
                       r.input_items, r.output_items, c.depth + 1 \
                  FROM responses r \
                  JOIN chain c ON r.response_id = c.previous_response_id \
                 WHERE c.depth < $3 \
             ) \
             SELECT response_id, previous_response_id, tenant_id, stored, \
                    input_items, output_items, depth \
               FROM chain ORDER BY depth DESC";

        let rows = sqlx::query(sql)
            .bind(from.to_string())
            .bind(tenant.as_str())
            .bind(limits.max_depth as i32)
            .fetch_all(&self.pool)
            .await
            .map_err(to_context_error)?;

        if rows.is_empty() {
            // The anchor is absent, or belongs to another tenant. Reported as a
            // broken chain rather than as a missing resource, because
            // `previous_response_id` is a request *field*, not the addressed
            // resource — and this keeps the mem and sql backends identical.
            return Err(ContextError::ChainBroken(from.to_string()));
        }

        // Rows arrive deepest-first, i.e. chronological order already.
        let mut items: Vec<ResponseItem> = Vec::new();
        let mut bytes = 0usize;
        let mut deepest = 0usize;
        let mut expected_previous: Option<String> = None;

        for row in &rows {
            let depth: i32 = row.try_get("depth").map_err(to_context_error)?;
            deepest = deepest.max(depth as usize);

            let row_tenant: String = row.try_get("tenant_id").map_err(to_context_error)?;
            if row_tenant != tenant.as_str() {
                return Err(ContextError::CrossTenant);
            }
            let stored: bool = row.try_get("stored").map_err(to_context_error)?;
            if !stored {
                return Err(ContextError::NotStored);
            }

            let inputs = items_from_json(row.try_get("input_items").map_err(to_context_error)?)
                .map_err(|e| ContextError::Internal(e.to_string()))?;
            let outputs = items_from_json(row.try_get("output_items").map_err(to_context_error)?)
                .map_err(|e| ContextError::Internal(e.to_string()))?;

            for item in inputs.into_iter().chain(outputs.into_iter()) {
                bytes = bytes.saturating_add(item.byte_len());
                if bytes > limits.max_bytes {
                    return Err(ContextError::ChainTooLarge {
                        limit: limits.max_bytes,
                    });
                }
                items.push(item);
                if items.len() > limits.max_items {
                    return Err(ContextError::ChainTooLong {
                        limit: limits.max_items,
                    });
                }
            }
            expected_previous = row
                .try_get::<Option<String>, _>("previous_response_id")
                .map_err(to_context_error)?;
        }

        // The last row walked is the oldest link. If it still points at a
        // predecessor, that predecessor was unreachable: either it does not
        // exist, or the depth bound cut the walk short. Distinguish the two so
        // the caller gets an accurate error rather than a truncated context.
        if let Some(missing) = expected_previous {
            if deepest >= limits.max_depth {
                return Err(ContextError::ChainTooLong {
                    limit: limits.max_depth,
                });
            }
            return Err(ContextError::ChainBroken(missing));
        }

        Ok(ResolvedContext {
            items,
            depth: deepest,
            bytes,
        })
    }

    async fn delete(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
    ) -> Result<bool, ContextError> {
        let affected = sqlx::query("DELETE FROM responses WHERE response_id = $1 AND tenant_id = $2")
            .bind(response_id.to_string())
            .bind(tenant.as_str())
            .execute(&self.pool)
            .await
            .map_err(to_context_error)?
            .rows_affected();
        Ok(affected > 0)
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
