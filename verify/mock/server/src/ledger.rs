//! Generation ledger over the shared in-memory store.
//!
//! Preserved from the previous meta store: single-point conditional claim
//! (INV-1), TTL-free idempotency gate (INV-2), monotonic attempts (INV-5),
//! reaping that raises the fence, read-only degrade, overload rejection.
//!
//! Gone: session rows, session locks, `Busy` outcomes.
//! New: cancel, node ownership, startup orphan reclaim, partial usage.

use std::sync::Arc;

use async_trait::async_trait;
use nova_responses::{
    AbortedClaim, AgentId, Attempt, ClaimedResponse, CreateOutcome, IdempotencyKey, LedgerError,
    ResponseId, ResponseLedger, ResponseStatus, StoredResponse, TenantId, Usage,
};

use crate::store::MemStore;

pub struct MemResponseLedger {
    store: Arc<MemStore>,
}

impl MemResponseLedger {
    pub fn new(store: Arc<MemStore>) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &Arc<MemStore> {
        &self.store
    }

    /// Synchronous fence check, used by the event log on the append path where
    /// an `.await` would mean releasing and reacquiring the lock.
    pub fn check_attempt_sync(
        &self,
        response_id: &ResponseId,
        attempt: Attempt,
    ) -> Result<(), LedgerError> {
        if self.store.is_read_only() {
            return Err(LedgerError::ReadOnly);
        }
        let g = self.store.lock();
        let Some(rec) = g.records.get(response_id) else {
            return Err(LedgerError::NotFound);
        };
        if rec.attempt != attempt {
            return Err(LedgerError::StaleAttempt);
        }
        if rec.status != ResponseStatus::InProgress {
            return Err(LedgerError::StaleAttempt);
        }
        Ok(())
    }

    fn guard_writable(&self) -> Result<(), LedgerError> {
        if self.store.is_read_only() {
            Err(LedgerError::ReadOnly)
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl ResponseLedger for MemResponseLedger {
    async fn create(
        &self,
        record: StoredResponse,
        idempotency_key: IdempotencyKey,
        _now_ms: u64,
    ) -> Result<CreateOutcome, LedgerError> {
        // A replay of an accepted key stays idempotent even while read-only:
        // the caller already got a success for it (INV-2).
        {
            let g = self.store.lock();
            if let Some(existing) = g.idem.get(&idempotency_key.0) {
                return Ok(CreateOutcome::Duplicate {
                    response_id: existing.clone(),
                });
            }
        }
        if self.store.is_read_only() {
            return Ok(CreateOutcome::ReadOnly);
        }

        let mut g = self.store.lock();
        if g.in_flight_count() >= self.store.pending_limit() {
            return Ok(CreateOutcome::Overloaded);
        }
        if g.records.len() >= self.store.max_records() {
            // Refuse rather than evict: dropping an existing record would break
            // a chain silently (INV-43).
            return Err(LedgerError::Unavailable);
        }
        let response_id = record.response_id.clone();
        g.queued.push_back(response_id.clone());
        g.idem.insert(idempotency_key.0, response_id.clone());
        g.insert_record(record);
        Ok(CreateOutcome::Accepted { response_id })
    }

    async fn claim(
        &self,
        agent_id: AgentId,
        now_ms: u64,
        exec_ttl_ms: u64,
    ) -> Result<Option<ClaimedResponse>, LedgerError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        // Pop-and-verify in one lock: the check and the transition are a single
        // atomic step, so two callers cannot both win (INV-1).
        //
        // Global claim (D25): no node filter — any execution process may take any
        // queued response, because the in-flight buffer is shared.
        loop {
            let Some(id) = g.queued.pop_front() else {
                return Ok(None);
            };
            let Some(rec) = g.records.get_mut(&id) else {
                continue; // deleted meanwhile
            };
            if rec.status != ResponseStatus::Queued {
                continue;
            }
            let attempt = rec.attempt.next();
            rec.attempt = attempt;
            rec.status = ResponseStatus::InProgress;
            rec.owner = Some(agent_id);
            let deadline = now_ms.saturating_add(exec_ttl_ms);
            let record = rec.clone();
            g.heartbeats.insert(agent_id, now_ms);
            return Ok(Some(ClaimedResponse {
                record,
                attempt,
                exec_deadline_ms: deadline,
            }));
        }
    }

    async fn heartbeat(&self, agent_id: AgentId, now_ms: u64) -> Result<(), LedgerError> {
        self.store.lock().heartbeats.insert(agent_id, now_ms);
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
        let mut g = self.store.lock();
        let Some(rec) = g.records.get_mut(response_id) else {
            return Err(LedgerError::NotFound);
        };
        if rec.attempt != expected_attempt {
            return Err(LedgerError::StaleAttempt);
        }
        if rec.status != ResponseStatus::InProgress {
            return Err(LedgerError::StaleAttempt);
        }
        rec.status = status;
        rec.owner = None;
        rec.usage = usage;
        rec.completed_at_ms = Some(now_ms);
        Ok(())
    }

    async fn cancel(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
        now_ms: u64,
    ) -> Result<(), LedgerError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        let Some(rec) = g.records.get_mut(response_id) else {
            return Err(LedgerError::NotFound);
        };
        // Cross-tenant looks identical to missing, so ids cannot be probed
        // (SEC-2); the edge maps both to 404.
        if &rec.tenant_id != tenant {
            return Err(LedgerError::NotFound);
        }
        if rec.status.is_terminal() {
            return Err(LedgerError::InvalidTransition(format!(
                "already {}",
                rec.status.as_str()
            )));
        }
        let attempt = rec.attempt;
        let partial = rec.usage;
        rec.status = ResponseStatus::Cancelled;
        rec.owner = None;
        rec.completed_at_ms = Some(now_ms);
        // Tokens already burnt on the running attempt still have to be booked
        // (INV-51), otherwise billing silently under-counts.
        if !partial.is_zero() {
            g.partial_usage
                .insert((response_id.clone(), attempt), partial);
        }
        Ok(())
    }

    async fn reap(
        &self,
        now_ms: u64,
        heartbeat_ttl_ms: u64,
    ) -> Result<Vec<AbortedClaim>, LedgerError> {
        let mut g = self.store.lock();
        let mut to_fail = Vec::new();
        for (id, rec) in g.records.iter() {
            if rec.status != ResponseStatus::InProgress {
                continue;
            }
            let lost = rec
                .owner
                .and_then(|a| g.heartbeats.get(&a).copied())
                .map(|hb| now_ms.saturating_sub(hb) > heartbeat_ttl_ms)
                .unwrap_or(true);
            if lost {
                to_fail.push(id.clone());
            }
        }

        let mut aborted = Vec::new();
        for id in to_fail {
            let Some(rec) = g.records.get_mut(&id) else {
                continue;
            };
            let previous_attempt = rec.attempt;
            let partial = rec.usage;
            // Read the association out while the record is in hand: the reap
            // caller needs it to release the turn lock, and a follow-up read
            // would be a second look at a row this loop already holds.
            let tenant_id = rec.tenant_id.clone();
            let conversation_id = rec.conversation_id.clone();
            // Raise the fence first so the stale holder's next append fails.
            rec.attempt = previous_attempt.next();
            rec.status = ResponseStatus::Failed;
            rec.owner = None;
            rec.completed_at_ms = Some(now_ms);
            if !partial.is_zero() {
                g.partial_usage
                    .insert((id.clone(), previous_attempt), partial);
            }
            aborted.push(AbortedClaim {
                response_id: id,
                previous_attempt,
                tenant_id,
                conversation_id,
            });
        }
        Ok(aborted)
    }

    async fn record_partial_usage(
        &self,
        response_id: &ResponseId,
        attempt: Attempt,
        usage: Usage,
    ) -> Result<(), LedgerError> {
        let mut g = self.store.lock();
        if !g.records.contains_key(response_id) {
            return Err(LedgerError::NotFound);
        }
        let entry = g
            .partial_usage
            .entry((response_id.clone(), attempt))
            .or_default();
        *entry = entry.add(usage);
        Ok(())
    }

    async fn get(&self, response_id: &ResponseId) -> Result<Option<StoredResponse>, LedgerError> {
        Ok(self.store.lock().records.get(response_id).cloned())
    }

    async fn check_attempt(
        &self,
        response_id: &ResponseId,
        attempt: Attempt,
    ) -> Result<(), LedgerError> {
        self.check_attempt_sync(response_id, attempt)
    }

    async fn in_flight(&self) -> Result<usize, LedgerError> {
        Ok(self.store.lock().in_flight_count())
    }

    fn set_read_only(&self, enabled: bool) {
        self.store.set_read_only(enabled);
    }

    fn is_read_only(&self) -> bool {
        self.store.is_read_only()
    }

    fn set_pending_limit(&self, limit: usize) {
        self.store.set_pending_limit(limit);
    }

    fn pending_limit(&self) -> usize {
        self.store.pending_limit()
    }
}

impl MemResponseLedger {
    /// Total usage including abandoned attempts, for assertions (CR-11).
    pub fn total_usage(&self, response_id: &ResponseId) -> Usage {
        self.store.lock().total_usage(response_id)
    }

    pub fn partial_usage_count(&self, response_id: &ResponseId) -> usize {
        self.store
            .lock()
            .partial_usage
            .keys()
            .filter(|(id, _)| id == response_id)
            .count()
    }
}
