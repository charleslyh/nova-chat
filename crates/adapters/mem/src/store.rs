//! Shared in-memory state behind the ledger and the context store.
//!
//! **Why one struct rather than two:** D21 ① requires the ledger and the
//! context store to share storage and a transaction, so a created response and
//! its stored items can never disagree. Modelling that as a single guarded map
//! makes the invariant structural rather than something callers must remember —
//! and it mirrors the SQL adapter, where both live in one table.
//!
//! Scope: **verification only** (L0–L2). Nothing here survives a restart and
//! nothing is shared between processes; production uses the sql adapter.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use nova_responses_core::{
    AgentId, Attempt, ResponseId, StoredResponse, TenantId, Usage,
};
use parking_lot::{Mutex, MutexGuard};

pub(crate) struct Inner {
    pub records: HashMap<ResponseId, StoredResponse>,
    /// Tenant secondary index, so bulk purge does not scan the whole map.
    pub by_tenant: HashMap<TenantId, BTreeSet<ResponseId>>,
    /// Expiry index keyed by deadline: sweeping is O(log n) per removal instead
    /// of a full table scan every tick.
    pub expiry: BTreeMap<(u64, ResponseId), ()>,
    pub queued: VecDeque<ResponseId>,
    /// Idempotency gate. No TTL window — presence alone rejects (INV-2).
    pub idem: HashMap<String, ResponseId>,
    pub heartbeats: HashMap<AgentId, u64>,
    /// Usage booked against abandoned attempts (INV-51).
    pub partial_usage: HashMap<(ResponseId, Attempt), Usage>,
}

pub struct MemStore {
    inner: Mutex<Inner>,
    read_only: AtomicBool,
    unavailable: AtomicBool,
    pending_limit: AtomicUsize,
    max_records: AtomicUsize,
}

impl MemStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                records: HashMap::new(),
                by_tenant: HashMap::new(),
                expiry: BTreeMap::new(),
                queued: VecDeque::new(),
                idem: HashMap::new(),
                heartbeats: HashMap::new(),
                partial_usage: HashMap::new(),
            }),
            read_only: AtomicBool::new(false),
            unavailable: AtomicBool::new(false),
            pending_limit: AtomicUsize::new(10_000),
            max_records: AtomicUsize::new(100_000),
        }
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock()
    }

    /// INV-32: read-only degrade rejects writes while reads keep working.
    pub fn set_read_only(&self, enabled: bool) {
        self.read_only.store(enabled, Ordering::SeqCst);
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only.load(Ordering::SeqCst)
    }

    pub fn set_pending_limit(&self, limit: usize) {
        self.pending_limit.store(limit.max(1), Ordering::SeqCst);
    }

    pub fn pending_limit(&self) -> usize {
        self.pending_limit.load(Ordering::SeqCst)
    }

    pub fn set_max_records(&self, limit: usize) {
        self.max_records.store(limit.max(1), Ordering::SeqCst);
    }

    pub fn max_records(&self) -> usize {
        self.max_records.load(Ordering::SeqCst)
    }

    /// Simulate the store being unreachable, to exercise the refuse-writes
    /// degrade (INV-46) without tearing down a real database.
    pub fn set_unavailable(&self, enabled: bool) {
        self.unavailable.store(enabled, Ordering::SeqCst);
    }

    pub fn is_unavailable(&self) -> bool {
        self.unavailable.load(Ordering::SeqCst)
    }
}

impl Default for MemStore {
    fn default() -> Self {
        Self::new()
    }
}

impl Inner {
    pub fn insert_record(&mut self, record: StoredResponse) {
        let id = record.response_id.clone();
        if let Some(deadline) = record.expires_at_ms {
            self.expiry.insert((deadline, id.clone()), ());
        }
        self.by_tenant
            .entry(record.tenant_id.clone())
            .or_default()
            .insert(id.clone());
        self.records.insert(id, record);
    }

    pub fn remove_record(&mut self, id: &ResponseId) -> Option<StoredResponse> {
        let record = self.records.remove(id)?;
        if let Some(deadline) = record.expires_at_ms {
            self.expiry.remove(&(deadline, id.clone()));
        }
        if let Some(set) = self.by_tenant.get_mut(&record.tenant_id) {
            set.remove(id);
            if set.is_empty() {
                self.by_tenant.remove(&record.tenant_id);
            }
        }
        self.partial_usage.retain(|(rid, _), _| rid != id);
        Some(record)
    }

    pub fn in_flight_count(&self) -> usize {
        self.records
            .values()
            .filter(|r| !r.status.is_terminal())
            .count()
    }

    /// Total usage for a response: the terminal figure plus everything booked
    /// against abandoned attempts.
    pub fn total_usage(&self, id: &ResponseId) -> Usage {
        let base = self
            .records
            .get(id)
            .map(|r| r.usage)
            .unwrap_or_default();
        self.partial_usage
            .iter()
            .filter(|((rid, _), _)| rid == id)
            .fold(base, |acc, (_, usage)| acc.add(*usage))
    }
}
