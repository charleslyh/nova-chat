//! Per-response bounded event buffer.
//!
//! Two design points that the previous session-scoped log did not have:
//!
//! **0-based contiguous sequence numbers (INV-11).** This is what makes
//! `starting_after=N` mean "give me N+1 onwards" with certainty. It also lets
//! reads index directly into the ring — `O(limit)` — instead of filtering the
//! whole event list on every poll, which was a real hotspot at 50 ms polling
//! over thousands of events.
//!
//! **Eviction is the only discontinuity, and it is always explicit (INV-40).**
//! When a ring fills we drop the oldest events and raise the evicted watermark.
//! Subscribers that fall behind get `Expired`; they are never silently resumed
//! from a later position, and there is no snapshot or cold tier to fall back
//! to. Note this is a deliberate choice over refusing the append: refusing
//! would escalate "subscriber buffer too small" into "generation failed",
//! whereas evicting keeps the generation running and its final output intact —
//! the stored items travel a separate path (INV-48).

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nova_responses::{
    AppendEvent, EventLogError, LedgerError, ResponseEvent, ResponseEventLog, ResponseId,
    ResponseLedger,
};
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::ledger::MemResponseLedger;

/// Retention multiplier after which an expired log degrades from a tombstone
/// (`Expired`, 410) to fully unknown (`Unknown`, 404).
const TOMBSTONE_FACTOR: u64 = 10;

struct ResponseLog {
    ring: VecDeque<ResponseEvent>,
    /// Next sequence number to assign. Starts at 0.
    next_seq: u64,
    /// Lowest sequence number still present. Reads below this are `Expired`.
    evicted_before: u64,
    terminal_at_ms: Option<u64>,
    retain_ms: u64,
    /// Ring emptied by the sweeper; the entry survives briefly so callers get a
    /// precise `Expired` rather than an ambiguous `Unknown`.
    swept: bool,
}

impl ResponseLog {
    fn new(capacity: usize) -> Self {
        Self {
            ring: VecDeque::with_capacity(capacity.min(1024)),
            next_seq: 0,
            evicted_before: 0,
            terminal_at_ms: None,
            retain_ms: 0,
            swept: false,
        }
    }

    fn tombstone_deadline(&self) -> Option<u64> {
        self.terminal_at_ms
            .map(|t| t.saturating_add(self.retain_ms.saturating_mul(TOMBSTONE_FACTOR)))
    }

    fn retention_deadline(&self) -> Option<u64> {
        self.terminal_at_ms.map(|t| t.saturating_add(self.retain_ms))
    }
}

struct Inner {
    logs: HashMap<ResponseId, ResponseLog>,
}

pub struct MemResponseEventLog {
    inner: Mutex<Inner>,
    notify: Notify,
    ledger: Arc<MemResponseLedger>,
    capacity_per_response: usize,
    max_logs: usize,
}

impl MemResponseEventLog {
    pub fn new(ledger: Arc<MemResponseLedger>) -> Self {
        Self::with_capacity(ledger, 20_000, 100_000)
    }

    pub fn with_capacity(
        ledger: Arc<MemResponseLedger>,
        capacity_per_response: usize,
        max_logs: usize,
    ) -> Self {
        Self {
            inner: Mutex::new(Inner {
                logs: HashMap::new(),
            }),
            notify: Notify::new(),
            ledger,
            capacity_per_response: capacity_per_response.max(1),
            max_logs: max_logs.max(1),
        }
    }

    /// Lowest retained sequence number, for assertions.
    pub fn evicted_before(&self, id: &ResponseId) -> Option<u64> {
        self.inner.lock().logs.get(id).map(|l| l.evicted_before)
    }

    pub fn buffered_len(&self, id: &ResponseId) -> usize {
        self.inner
            .lock()
            .logs
            .get(id)
            .map(|l| l.ring.len())
            .unwrap_or(0)
    }

    pub fn log_count(&self) -> usize {
        self.inner.lock().logs.len()
    }
}

#[async_trait]
impl ResponseEventLog for MemResponseEventLog {
    async fn append(&self, event: AppendEvent) -> Result<u64, EventLogError> {
        if self.ledger.is_read_only() {
            return Err(EventLogError::ReadOnly);
        }
        // Attempt fence before the write (INV-6): a reaped holder must not be
        // able to inject events after its attempt was superseded.
        if let Some(attempt) = event.attempt {
            self.ledger
                .check_attempt_sync(&event.response_id, attempt)
                .map_err(|e| match e {
                    LedgerError::StaleAttempt => EventLogError::StaleAttempt,
                    LedgerError::ReadOnly => EventLogError::ReadOnly,
                    LedgerError::NotFound => EventLogError::Unknown,
                    other => EventLogError::Internal(other.to_string()),
                })?;
        }

        let seq = {
            let mut g = self.inner.lock();
            if !g.logs.contains_key(&event.response_id) && g.logs.len() >= self.max_logs {
                // Node-level memory protection. Distinct from per-response
                // eviction: here we refuse to take on new work at all.
                return Err(EventLogError::CapacityExceeded);
            }
            let log = g
                .logs
                .entry(event.response_id.clone())
                .or_insert_with(|| ResponseLog::new(self.capacity_per_response));
            if log.swept {
                return Err(EventLogError::Expired);
            }
            let seq = log.next_seq;
            log.next_seq += 1;
            log.ring.push_back(event.with_seq(seq));
            while log.ring.len() > self.capacity_per_response {
                log.ring.pop_front();
                log.evicted_before += 1;
            }
            seq
        };
        self.notify.notify_waiters();
        Ok(seq)
    }

    async fn read_after(
        &self,
        response_id: &ResponseId,
        starting_after: Option<u64>,
        limit: usize,
        wait_ms: u64,
    ) -> Result<Vec<ResponseEvent>, EventLogError> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms);
        loop {
            {
                let g = self.inner.lock();
                let Some(log) = g.logs.get(response_id) else {
                    return Err(EventLogError::Unknown);
                };
                if log.swept {
                    return Err(EventLogError::Expired);
                }
                // Target is the first sequence number the caller still needs.
                // `None` means from the very beginning, i.e. sequence 0 — which
                // is itself a valid number, hence the Option rather than a
                // sentinel.
                let target = match starting_after {
                    None => 0,
                    Some(after) => after.saturating_add(1),
                };
                if target < log.evicted_before {
                    return Err(EventLogError::Expired);
                }
                if target < log.next_seq {
                    // Direct index: contiguity guarantees position.
                    let offset = (target - log.evicted_before) as usize;
                    let batch: Vec<_> = log
                        .ring
                        .iter()
                        .skip(offset)
                        .take(limit)
                        .cloned()
                        .collect();
                    if !batch.is_empty() {
                        return Ok(batch);
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(vec![]);
            }
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {}
            }
        }
    }

    async fn close(
        &self,
        response_id: &ResponseId,
        now_ms: u64,
        retain_ms: u64,
    ) -> Result<(), EventLogError> {
        let mut g = self.inner.lock();
        let Some(log) = g.logs.get_mut(response_id) else {
            return Err(EventLogError::Unknown);
        };
        log.retain_ms = retain_ms;
        log.terminal_at_ms = Some(now_ms);
        Ok(())
    }

    async fn remove(&self, response_id: &ResponseId) -> Result<(), EventLogError> {
        let mut g = self.inner.lock();
        if g.logs.remove(response_id).is_none() {
            return Err(EventLogError::Unknown);
        }
        Ok(())
    }

    async fn sweep_expired(&self, now_ms: u64) -> Result<u64, EventLogError> {
        let mut swept = 0u64;
        let mut g = self.inner.lock();
        let mut drop_ids = Vec::new();
        for (id, log) in g.logs.iter_mut() {
            if let Some(deadline) = log.tombstone_deadline() {
                if now_ms >= deadline {
                    drop_ids.push(id.clone());
                    continue;
                }
            }
            if log.swept {
                continue;
            }
            if let Some(deadline) = log.retention_deadline() {
                if now_ms >= deadline {
                    log.ring.clear();
                    log.ring.shrink_to_fit();
                    log.evicted_before = log.next_seq;
                    log.swept = true;
                    swept += 1;
                }
            }
        }
        for id in drop_ids {
            g.logs.remove(&id);
            swept += 1;
        }
        Ok(swept)
    }
}
