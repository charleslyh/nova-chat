//! Generation ledger over the shared in-memory store.
//!
//! Single-point conditional claim (INV-1), TTL-free idempotency gate (INV-2), monotonic
//! attempts (INV-5), reaping that raises the fence, read-only degrade, overload rejection.
//!
//! The node-local degrade switches are a separate impl block ([`AdmissionControl`]) from
//! the persistence operations, matching the port split: flipping `read_only` has nothing
//! to do with writing a row.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nova_responses::ports::{
    AbortedClaim, AdmissionControl, ClaimedResponse, ContentIntegrity, CreateOutcome, LedgerError,
    ResponseLedger, StoreError,
};
use nova_responses::{
    canonical_items, AgentId, Attempt, IdempotencyKey, IntegrityTag, ResponseId, ResponseItem,
    ResponseRecord, ResponseStatus, TenantId, Usage,
};

use crate::store::MemStore;

pub struct MemResponseLedger {
    store: Arc<MemStore>,
    integrity: Option<Arc<dyn ContentIntegrity>>,
}

impl MemResponseLedger {
    pub fn new(store: Arc<MemStore>) -> Self {
        Self {
            store,
            integrity: None,
        }
    }

    pub fn with_integrity(
        store: Arc<MemStore>,
        integrity: Option<Arc<dyn ContentIntegrity>>,
    ) -> Self {
        Self { store, integrity }
    }

    pub fn store(&self) -> &Arc<MemStore> {
        &self.store
    }

    /// Synchronous fence check, used by the event log on the append path where an
    /// `.await` would mean releasing and reacquiring the lock.
    pub fn check_attempt_sync(
        &self,
        response_id: &ResponseId,
        attempt: Attempt,
    ) -> Result<(), LedgerError> {
        if self.store.is_read_only() {
            return Err(LedgerError::Store(StoreError::ReadOnly));
        }
        let g = self.store.lock();
        let Some(rec) = g.records.get(response_id) else {
            return Err(LedgerError::NotFound);
        };
        if rec.attempt != attempt || rec.status != ResponseStatus::InProgress {
            return Err(LedgerError::StaleAttempt);
        }
        Ok(())
    }

    fn guard_writable(&self) -> Result<(), LedgerError> {
        if self.store.is_read_only() {
            Err(LedgerError::Store(StoreError::ReadOnly))
        } else {
            Ok(())
        }
    }

    /// Sign the stored input so tampering is detectable (CR-13).
    fn sign(&self, record: &mut ResponseRecord) -> Result<(), LedgerError> {
        let Some(integrity) = &self.integrity else {
            return Ok(());
        };
        let canonical = canonical_items(&record.spec.input_items);
        let tag = integrity
            .sign(&canonical)
            .map_err(|e| LedgerError::Store(StoreError::Internal(e.to_string())))?;
        record.integrity = Some(IntegrityTag {
            alg: integrity.alg().to_string(),
            tag,
        });
        Ok(())
    }
}

#[async_trait]
impl ResponseLedger for MemResponseLedger {
    async fn create(
        &self,
        record: ResponseRecord,
        idempotency_key: IdempotencyKey,
        _now_ms: u64,
    ) -> Result<CreateOutcome, LedgerError> {
        // Signing needs no lock, so it happens first and the critical section below stays
        // a single uninterrupted decision.
        let mut record = record;
        self.sign(&mut record)?;

        // **One lock for the whole decision** (INV-2). Checking the idempotency gate and
        // inserting under two separate locks lets four concurrent retries of one key each
        // miss the check and each insert — which is exactly the case the gate exists for,
        // since concurrent retries are what a client that timed out does.
        let mut g = self.store.lock();

        // A replay of an accepted key stays idempotent even while read-only: the caller
        // already got a success for it. The original record is returned, so the caller
        // never has to read back a row this statement already had in hand.
        if let Some(existing) = g.idem.get(&idempotency_key) {
            let existing = g
                .records
                .get(existing)
                .cloned()
                .ok_or(LedgerError::NotFound)?;
            return Ok(CreateOutcome::duplicate(existing));
        }
        if self.store.is_read_only() {
            return Ok(CreateOutcome::ReadOnly);
        }
        if g.in_flight_count() >= self.store.pending_limit() {
            return Ok(CreateOutcome::Overloaded);
        }
        if g.records.len() >= self.store.max_records() {
            // Refuse rather than evict: dropping an existing record would break a chain
            // silently (INV-43).
            return Err(LedgerError::Store(StoreError::Unavailable));
        }
        g.queued.push_back(record.response_id.clone());
        g.idem.insert(idempotency_key, record.response_id.clone());
        g.insert_record(record.clone());
        Ok(CreateOutcome::accepted(record))
    }

    async fn claim(
        &self,
        agent_id: AgentId,
        now_ms: u64,
        exec_ttl: Duration,
    ) -> Result<Option<ClaimedResponse>, LedgerError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        // Pop-and-verify in one lock: the check and the transition are a single atomic
        // step, so two callers cannot both win (INV-1).
        //
        // Global claim (D25): no node filter — any execution process may take any queued
        // response, because the in-flight buffer is shared.
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
            rec.attempt = rec.attempt.next();
            rec.status = ResponseStatus::InProgress;
            rec.owner = Some(agent_id);
            let record = rec.clone();
            g.heartbeats.insert(agent_id, now_ms);
            return Ok(Some(ClaimedResponse {
                record,
                exec_deadline_ms: now_ms.saturating_add(exec_ttl.as_millis() as u64),
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
        if rec.attempt != expected_attempt || rec.status != ResponseStatus::InProgress {
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
        // Cross-tenant looks identical to missing, so ids cannot be probed (SEC-2); the
        // edge maps both to 404.
        if &rec.tenant_id != tenant {
            return Err(LedgerError::NotFound);
        }
        if rec.status.is_terminal() {
            return Err(LedgerError::InvalidTransition(format!(
                "already {}",
                rec.status.as_str()
            )));
        }
        let previous_attempt = rec.attempt;
        let partial = rec.usage;
        // Raise the fence first so the executing agent observes the cancellation — its
        // next append and its active cancellation probe both see StaleAttempt — and
        // stops spending tokens promptly rather than running the ReAct loop out.
        rec.attempt = previous_attempt.next();
        rec.status = ResponseStatus::Cancelled;
        rec.owner = None;
        rec.completed_at_ms = Some(now_ms);
        // Tokens already burnt on the running attempt still have to be booked (INV-51),
        // otherwise billing silently under-counts. Booked against the superseded attempt.
        if !partial.is_zero() {
            g.partial_usage
                .insert((response_id.clone(), previous_attempt), partial);
        }
        Ok(())
    }

    async fn reap(
        &self,
        now_ms: u64,
        heartbeat_ttl: Duration,
    ) -> Result<Vec<AbortedClaim>, LedgerError> {
        let ttl_ms = heartbeat_ttl.as_millis() as u64;
        let mut g = self.store.lock();
        let mut to_fail = Vec::new();
        for (id, rec) in g.records.iter() {
            if rec.status != ResponseStatus::InProgress {
                continue;
            }
            let lost = rec
                .owner
                .and_then(|a| g.heartbeats.get(&a).copied())
                .map(|hb| now_ms.saturating_sub(hb) > ttl_ms)
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
            // Read the association out while the record is in hand: the reap caller needs
            // it to release the turn lock, and a follow-up read would be a second look at
            // a row this loop already holds.
            let tenant_id = rec.tenant_id.clone();
            let conversation_id = rec.conversation_id().cloned();
            // The turn's own input and store flag travel with the claim so the sweeper can
            // archive the input to the conversation snapshot (D30 incomplete-turn archival)
            // without a follow-up read.
            let input_items = rec.spec.input_items.clone();
            let store = rec.spec.store;
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
                input_items,
                store,
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
        *entry = entry.accumulate(usage);
        Ok(())
    }

    async fn get(&self, response_id: &ResponseId) -> Result<Option<ResponseRecord>, LedgerError> {
        Ok(self.store.lock().records.get(response_id).cloned())
    }

    async fn delete(&self, response_id: &ResponseId) -> Result<bool, LedgerError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        Ok(g.remove_record(response_id).is_some())
    }

    async fn delete_by_tenant(&self, tenant: &TenantId) -> Result<u64, LedgerError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        let ids: Vec<ResponseId> = g
            .by_tenant
            .get(tenant)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default();
        let mut removed = 0u64;
        for id in ids {
            if g.remove_record(&id).is_some() {
                removed += 1;
            }
        }
        Ok(removed)
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
}

impl AdmissionControl for MemResponseLedger {
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

    /// Test hook: corrupt stored input without updating the tag, to prove tampering is
    /// detectable (CR-13).
    pub fn tamper_for_test(&self, response_id: &ResponseId, replacement: Vec<ResponseItem>) -> bool {
        let mut g = self.store.lock();
        match g.records.get_mut(response_id) {
            Some(rec) => {
                rec.spec.input_items = replacement;
                true
            }
            None => false,
        }
    }

    pub fn record_count(&self) -> usize {
        self.store.lock().records.len()
    }
}
