//! PostgreSQL response ledger.

use async_trait::async_trait;
use nova_responses_core::{
    AbortedClaim, AgentId, Attempt, ClaimedResponse, CreateOutcome, IdempotencyKey, LedgerError,
    ResponseId, ResponseLedger, ResponseStatus, SessionId, StoredResponse, TenantId, Usage,
};
use sqlx::{PgPool, Row};

use crate::error::{is_unique_violation, to_ledger_error};
use crate::row::{
    items_to_json, partial_usage_from_json, partial_usage_to_json, record_from_row, status_to_str,
    total_usage, usage_from_json, usage_to_json, RECORD_COLUMNS,
};

pub struct SqlResponseLedger {
    pool: PgPool,
    read_only: std::sync::atomic::AtomicBool,
    pending_limit: std::sync::atomic::AtomicUsize,
}

impl SqlResponseLedger {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            read_only: std::sync::atomic::AtomicBool::new(false),
            pending_limit: std::sync::atomic::AtomicUsize::new(10_000),
        }
    }

    fn guard_writable(&self) -> Result<(), LedgerError> {
        if self.is_read_only() {
            Err(LedgerError::ReadOnly)
        } else {
            Ok(())
        }
    }

    async fn find_by_idempotency_key(
        &self,
        key: &str,
    ) -> Result<Option<ResponseId>, LedgerError> {
        let row = sqlx::query("SELECT response_id FROM responses WHERE idempotency_key = $1")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(to_ledger_error)?;
        match row {
            None => Ok(None),
            Some(row) => {
                let raw: String = row.try_get("response_id").map_err(to_ledger_error)?;
                ResponseId::parse(&raw)
                    .map(Some)
                    .map_err(|e| LedgerError::Internal(e.to_string()))
            }
        }
    }
}

#[async_trait]
impl ResponseLedger for SqlResponseLedger {
    async fn create(
        &self,
        record: StoredResponse,
        idempotency_key: IdempotencyKey,
        _now_ms: u64,
    ) -> Result<CreateOutcome, LedgerError> {
        // Replay of an accepted key stays idempotent even in read-only: the
        // caller already received success for it (INV-2).
        if let Some(existing) = self.find_by_idempotency_key(&idempotency_key.0).await? {
            return Ok(CreateOutcome::Duplicate {
                response_id: existing,
            });
        }
        if self.is_read_only() {
            return Ok(CreateOutcome::ReadOnly);
        }

        let in_flight = self.in_flight().await?;
        if in_flight >= self.pending_limit() {
            return Ok(CreateOutcome::Overloaded);
        }

        // Single INSERT: ledger state and stored items land in one row, so they
        // cannot disagree (D21 ①).
        let sql = "INSERT INTO responses (\
                response_id, previous_response_id, tenant_id, model, status, stored, node_tag, \
                attempt, owner, idempotency_key, instructions, input_items, output_items, usage, \
                partial_usage, integrity, integrity_alg, created_at_ms, completed_at_ms, expires_at_ms, \
                conversation_id, session_id) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'{}'::jsonb,$15,$16,$17,$18,$19,$20,$21)";
        let result = sqlx::query(sql)
            .bind(record.response_id.to_string())
            .bind(record.previous_response_id.as_ref().map(|v| v.to_string()))
            .bind(record.tenant_id.as_str())
            .bind(&record.model)
            .bind(status_to_str(record.status))
            .bind(record.stored)
            .bind(record.node_tag.as_str())
            .bind(record.attempt.0 as i64)
            .bind(record.owner.map(|v| v.to_string()))
            .bind(&idempotency_key.0)
            .bind(record.instructions.as_deref())
            .bind(items_to_json(&record.input_items))
            .bind(items_to_json(&record.output_items))
            .bind(usage_to_json(&record.usage))
            .bind(record.integrity.as_deref())
            .bind(record.integrity_alg.as_deref())
            .bind(record.created_at_ms as i64)
            .bind(record.completed_at_ms.map(|v| v as i64))
            .bind(record.expires_at_ms.map(|v| v as i64))
            .bind(record.conversation_id.as_ref().map(|v| v.to_string()))
            .bind(record.session_id.as_ref().map(|v| v.to_string()))
            .execute(&self.pool)
            .await;

        match result {
            Ok(_) => Ok(CreateOutcome::Accepted {
                response_id: record.response_id,
            }),
            // Lost a race on the same key: the unique constraint is the real
            // gate, the pre-read above is only a fast path.
            Err(e) if is_unique_violation(&e) => {
                match self.find_by_idempotency_key(&idempotency_key.0).await? {
                    Some(existing) => Ok(CreateOutcome::Duplicate {
                        response_id: existing,
                    }),
                    None => Err(LedgerError::Internal(
                        "unique violation without a matching row".into(),
                    )),
                }
            }
            Err(e) => Err(to_ledger_error(e)),
        }
    }

    async fn claim(
        &self,
        agent_id: AgentId,
        now_ms: u64,
        exec_ttl_ms: u64,
    ) -> Result<Option<ClaimedResponse>, LedgerError> {
        self.guard_writable()?;
        // `FOR UPDATE SKIP LOCKED` inside the sub-select makes selection and
        // transition a single atomic step, so two callers can never both win
        // (INV-1). Attempt is incremented in the same statement (INV-5).
        //
        // Global claim (D25): no `node_tag` filter — any execution process may
        // take any queued response, because the in-flight buffer is shared.
        let sql = format!(
            "UPDATE responses SET status = 'in_progress', attempt = attempt + 1, \
                    owner = $1, exec_deadline_ms = $2 \
              WHERE response_id = ( \
                    SELECT response_id FROM responses \
                     WHERE status = 'queued' \
                     ORDER BY created_at_ms \
                     LIMIT 1 \
                     FOR UPDATE SKIP LOCKED ) \
              RETURNING {RECORD_COLUMNS}"
        );
        let deadline = now_ms.saturating_add(exec_ttl_ms);
        let row = sqlx::query(&sql)
            .bind(agent_id.to_string())
            .bind(deadline as i64)
            .fetch_optional(&self.pool)
            .await
            .map_err(to_ledger_error)?;

        let Some(row) = row else {
            return Ok(None);
        };
        let record = record_from_row(&row).map_err(|e| LedgerError::Internal(e.to_string()))?;
        self.heartbeat(agent_id, now_ms).await?;
        Ok(Some(ClaimedResponse {
            attempt: record.attempt,
            record,
            exec_deadline_ms: deadline,
        }))
    }

    async fn heartbeat(&self, agent_id: AgentId, now_ms: u64) -> Result<(), LedgerError> {
        sqlx::query(
            "INSERT INTO agent_heartbeats (agent_id, last_seen_ms) VALUES ($1, $2) \
             ON CONFLICT (agent_id) DO UPDATE SET last_seen_ms = EXCLUDED.last_seen_ms",
        )
        .bind(agent_id.to_string())
        .bind(now_ms as i64)
        .execute(&self.pool)
        .await
        .map_err(to_ledger_error)?;
        Ok(())
    }

    async fn complete(
        &self,
        response_id: &ResponseId,
        expected_attempt: Attempt,
        status: ResponseStatus,
        usage: Usage,
        now_ms: u64,
    ) -> Result<(), LedgerError> {
        self.guard_writable()?;
        if !status.is_terminal() {
            return Err(LedgerError::InvalidTransition(format!(
                "{} is not terminal",
                status.as_str()
            )));
        }
        // The attempt predicate is the fence: a superseded holder updates zero
        // rows rather than overwriting the current attempt's result (INV-6).
        let affected = sqlx::query(
            "UPDATE responses SET status = $1, usage = $2, owner = NULL, \
                    exec_deadline_ms = NULL, completed_at_ms = $3 \
              WHERE response_id = $4 AND attempt = $5 AND status = 'in_progress'",
        )
        .bind(status_to_str(status))
        .bind(usage_to_json(&usage))
        .bind(now_ms as i64)
        .bind(response_id.to_string())
        .bind(expected_attempt.0 as i64)
        .execute(&self.pool)
        .await
        .map_err(to_ledger_error)?
        .rows_affected();

        if affected == 0 {
            return match self.get(response_id).await? {
                None => Err(LedgerError::NotFound),
                Some(_) => Err(LedgerError::StaleAttempt),
            };
        }
        Ok(())
    }

    async fn cancel(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
        now_ms: u64,
    ) -> Result<(), LedgerError> {
        self.guard_writable()?;
        let sql = format!(
            "SELECT {RECORD_COLUMNS}, partial_usage FROM responses \
              WHERE response_id = $1 AND tenant_id = $2"
        );
        let row = sqlx::query(&sql)
            .bind(response_id.to_string())
            .bind(tenant.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(to_ledger_error)?;
        // Cross-tenant is reported as missing so ids cannot be probed (SEC-2).
        let Some(row) = row else {
            return Err(LedgerError::NotFound);
        };
        let record = record_from_row(&row).map_err(|e| LedgerError::Internal(e.to_string()))?;
        if record.status.is_terminal() {
            return Err(LedgerError::InvalidTransition(format!(
                "already {}",
                record.status.as_str()
            )));
        }

        // Tokens burnt by the running attempt are booked before the transition,
        // otherwise cancelling would silently under-bill (INV-51).
        let mut partial = partial_usage_from_json(
            row.try_get("partial_usage").map_err(to_ledger_error)?,
        );
        if !record.usage.is_zero() {
            let key = record.attempt.0.to_string();
            let entry = partial.entry(key).or_default();
            *entry = entry.add(record.usage);
        }

        sqlx::query(
            "UPDATE responses SET status = 'cancelled', owner = NULL, exec_deadline_ms = NULL, \
                    completed_at_ms = $1, partial_usage = $2 \
              WHERE response_id = $3 AND tenant_id = $4",
        )
        .bind(now_ms as i64)
        .bind(partial_usage_to_json(&partial))
        .bind(response_id.to_string())
        .bind(tenant.as_str())
        .execute(&self.pool)
        .await
        .map_err(to_ledger_error)?;
        Ok(())
    }

    async fn reap(
        &self,
        now_ms: u64,
        heartbeat_ttl_ms: u64,
    ) -> Result<Vec<AbortedClaim>, LedgerError> {
        // Raise the fence and fail in one statement so a stale holder cannot
        // append between the two.
        // Concatenated rather than `format!`-ed: the statement contains
        // `'{}'::jsonb`, which a format string would try to interpret.
        let sql = "UPDATE responses SET attempt = attempt + 1, status = 'failed', \
                          owner = NULL, exec_deadline_ms = NULL, completed_at_ms = $1, \
                          partial_usage = CASE \
                              WHEN usage = '{}'::jsonb THEN partial_usage \
                              ELSE jsonb_set(partial_usage, ARRAY[attempt::text], usage, true) \
                          END \
                    WHERE status = 'in_progress' \
                      AND ( \
                          ( exec_deadline_ms IS NOT NULL AND exec_deadline_ms <= $1 ) \
                          OR owner IS NULL \
                          OR NOT EXISTS ( \
                              SELECT 1 FROM agent_heartbeats h \
                               WHERE h.agent_id = responses.owner \
                                 AND $1 - h.last_seen_ms <= $2 ) \
                      ) \
                    RETURNING "
            .to_string()
            + ABORTED_COLUMNS;
        let rows = sqlx::query(&sql)
            .bind(now_ms as i64)
            .bind(heartbeat_ttl_ms as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(to_ledger_error)?;
        collect_aborted(rows)
    }

    async fn record_partial_usage(
        &self,
        response_id: &ResponseId,
        attempt: Attempt,
        usage: Usage,
    ) -> Result<(), LedgerError> {
        let row = sqlx::query("SELECT partial_usage FROM responses WHERE response_id = $1")
            .bind(response_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(to_ledger_error)?;
        let Some(row) = row else {
            return Err(LedgerError::NotFound);
        };
        let mut partial =
            partial_usage_from_json(row.try_get("partial_usage").map_err(to_ledger_error)?);
        let entry = partial.entry(attempt.0.to_string()).or_default();
        *entry = entry.add(usage);

        sqlx::query("UPDATE responses SET partial_usage = $1 WHERE response_id = $2")
            .bind(partial_usage_to_json(&partial))
            .bind(response_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(to_ledger_error)?;
        Ok(())
    }

    async fn get(&self, response_id: &ResponseId) -> Result<Option<StoredResponse>, LedgerError> {
        let sql = format!("SELECT {RECORD_COLUMNS} FROM responses WHERE response_id = $1");
        let row = sqlx::query(&sql)
            .bind(response_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(to_ledger_error)?;
        match row {
            None => Ok(None),
            Some(row) => record_from_row(&row)
                .map(Some)
                .map_err(|e| LedgerError::Internal(e.to_string())),
        }
    }

    async fn check_attempt(
        &self,
        response_id: &ResponseId,
        attempt: Attempt,
    ) -> Result<(), LedgerError> {
        if self.is_read_only() {
            return Err(LedgerError::ReadOnly);
        }
        let row = sqlx::query("SELECT attempt, status FROM responses WHERE response_id = $1")
            .bind(response_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(to_ledger_error)?;
        let Some(row) = row else {
            return Err(LedgerError::NotFound);
        };
        let current: i64 = row.try_get("attempt").map_err(to_ledger_error)?;
        let status: String = row.try_get("status").map_err(to_ledger_error)?;
        if current as u64 != attempt.0 || status != "in_progress" {
            return Err(LedgerError::StaleAttempt);
        }
        Ok(())
    }

    async fn in_flight(&self) -> Result<usize, LedgerError> {
        let row = sqlx::query(
            "SELECT COUNT(*) AS n FROM responses WHERE status IN ('queued', 'in_progress')",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(to_ledger_error)?;
        let n: i64 = row.try_get("n").map_err(to_ledger_error)?;
        Ok(n.max(0) as usize)
    }

    fn set_read_only(&self, enabled: bool) {
        self.read_only
            .store(enabled, std::sync::atomic::Ordering::SeqCst);
    }

    fn is_read_only(&self) -> bool {
        self.read_only.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn set_pending_limit(&self, limit: usize) {
        self.pending_limit
            .store(limit.max(1), std::sync::atomic::Ordering::SeqCst);
    }

    fn pending_limit(&self) -> usize {
        self.pending_limit.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl SqlResponseLedger {
    /// Terminal usage plus everything booked against abandoned attempts.
    pub async fn total_usage(&self, response_id: &ResponseId) -> Result<Usage, LedgerError> {
        let row = sqlx::query("SELECT usage, partial_usage FROM responses WHERE response_id = $1")
            .bind(response_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(to_ledger_error)?;
        let Some(row) = row else {
            return Err(LedgerError::NotFound);
        };
        let base = usage_from_json(row.try_get("usage").map_err(to_ledger_error)?);
        let partial =
            partial_usage_from_json(row.try_get("partial_usage").map_err(to_ledger_error)?);
        Ok(total_usage(base, &partial))
    }
}

/// Columns every `RETURNING` clause feeding [`collect_aborted`] must produce.
///
/// Named once so the two call sites cannot return different column sets and only
/// fail at decode time, on the reap path, in production.
const ABORTED_COLUMNS: &str =
    "response_id, attempt - 1 AS previous_attempt, tenant_id, session_id";

fn collect_aborted(rows: Vec<sqlx::postgres::PgRow>) -> Result<Vec<AbortedClaim>, LedgerError> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let raw: String = row.try_get("response_id").map_err(to_ledger_error)?;
        let previous: i64 = row.try_get("previous_attempt").map_err(to_ledger_error)?;
        let tenant_raw: String = row.try_get("tenant_id").map_err(to_ledger_error)?;
        let session_raw: Option<String> = row.try_get("session_id").map_err(to_ledger_error)?;
        out.push(AbortedClaim {
            response_id: ResponseId::parse(&raw)
                .map_err(|e| LedgerError::Internal(e.to_string()))?,
            previous_attempt: Attempt(previous.max(0) as u64),
            tenant_id: TenantId::parse(&tenant_raw)
                .map_err(|e| LedgerError::Internal(format!("tenant_id: {e}")))?,
            session_id: match session_raw {
                None => None,
                Some(raw) => Some(
                    SessionId::parse(&raw)
                        .map_err(|e| LedgerError::Internal(format!("session_id: {e}")))?,
                ),
            },
        });
    }
    Ok(out)
}
