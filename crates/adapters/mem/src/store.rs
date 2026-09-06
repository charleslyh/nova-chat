//! Shared in-memory state behind the ledger and the context store.
//!
//! **Why one struct rather than two:** D21 ① requires the ledger and the
//! context store to share storage and a transaction, so a created response and
//! its stored items can never disagree. Modelling that as a single guarded map
//! makes the invariant structural rather than something callers must remember —
//! and it mirrors the SQL adapter, where both live in one table.
//!
//! Scope: **verification only** (L0–L2). Nothing here survives a restart;
//! production uses the sql adapter. When mounted in the carrier process it is
//! shared across processes by construction — the same state backs every client.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use nova_responses_core::{
    AgentId, Attempt, Conversation, ConversationId, ResponseId, Session, SessionEvent, SessionId,
    StoredResponse, TenantId, Usage,
};
use parking_lot::{Mutex, MutexGuard};

/// Session state plus its event stream, in one entry.
///
/// Held together for the same reason the SQL adapter keeps them in one row and
/// one transaction: taking the turn lock and appending the event that announces
/// it must be a single step. Under this mutex that is automatic — which is the
/// point of putting them here rather than in a store of their own.
pub(crate) struct SessionRow {
    pub session: Session,
    /// Next sequence to hand out. 0-based and contiguous (INV-11), so `events`
    /// can be indexed directly instead of scanned.
    pub next_seq: u64,
    pub events: Vec<SessionEvent>,
    /// Sequence assigned to the current turn's `TurnStarted`, so a re-entrant
    /// `begin_turn` returns it rather than appending a duplicate.
    pub lock_seq: Option<u64>,
    /// The last completed turn and the sequence its `TurnCompleted` got, making
    /// `end_turn` idempotent across execution-side retries.
    pub last_completed: Option<(ResponseId, u64)>,
}

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

    /// Conversations: a pointer to the tail of a response chain each, with no
    /// items of their own (D27).
    pub conversations: HashMap<ConversationId, Conversation>,
    pub conversations_by_tenant: HashMap<TenantId, BTreeSet<ConversationId>>,

    /// Sessions and their event streams (D26). Under the same mutex as
    /// `records`, so a turn boundary and the response it refers to cannot be
    /// observed out of step.
    pub sessions: HashMap<SessionId, SessionRow>,
    pub sessions_by_tenant: HashMap<TenantId, BTreeSet<SessionId>>,
}

pub struct MemStore {
    inner: Mutex<Inner>,
    read_only: AtomicBool,
    unavailable: AtomicBool,
    pending_limit: AtomicUsize,
    max_records: AtomicUsize,
    max_conversations: AtomicUsize,
    max_sessions: AtomicUsize,
    max_events_per_session: AtomicUsize,
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
                conversations: HashMap::new(),
                conversations_by_tenant: HashMap::new(),
                sessions: HashMap::new(),
                sessions_by_tenant: HashMap::new(),
            }),
            read_only: AtomicBool::new(false),
            unavailable: AtomicBool::new(false),
            pending_limit: AtomicUsize::new(10_000),
            max_records: AtomicUsize::new(100_000),
            max_conversations: AtomicUsize::new(100_000),
            max_sessions: AtomicUsize::new(100_000),
            max_events_per_session: AtomicUsize::new(100_000),
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

    pub fn set_max_conversations(&self, limit: usize) {
        self.max_conversations.store(limit.max(1), Ordering::SeqCst);
    }

    pub fn max_conversations(&self) -> usize {
        self.max_conversations.load(Ordering::SeqCst)
    }

    pub fn set_max_sessions(&self, limit: usize) {
        self.max_sessions.store(limit.max(1), Ordering::SeqCst);
    }

    pub fn max_sessions(&self) -> usize {
        self.max_sessions.load(Ordering::SeqCst)
    }

    /// Upper bound on one session's event stream.
    ///
    /// Reaching it **refuses the append** rather than evicting the oldest events,
    /// unlike the per-response event ring. The two differ because what they hold
    /// differs: dropping a token delta costs a subscriber some replay, while
    /// dropping a turn boundary or a business event loses the only record that it
    /// happened.
    pub fn set_max_events_per_session(&self, limit: usize) {
        self.max_events_per_session
            .store(limit.max(1), Ordering::SeqCst);
    }

    pub fn max_events_per_session(&self) -> usize {
        self.max_events_per_session.load(Ordering::SeqCst)
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
        Self::unindex(&mut self.by_tenant, &record.tenant_id, id);
        self.partial_usage.retain(|(rid, _), _| rid != id);
        Some(record)
    }

    pub fn insert_conversation(&mut self, conversation: Conversation) {
        self.conversations_by_tenant
            .entry(conversation.tenant_id.clone())
            .or_default()
            .insert(conversation.id.clone());
        self.conversations
            .insert(conversation.id.clone(), conversation);
    }

    pub fn remove_conversation(&mut self, id: &ConversationId) -> Option<Conversation> {
        let conversation = self.conversations.remove(id)?;
        Self::unindex(
            &mut self.conversations_by_tenant,
            &conversation.tenant_id,
            id,
        );
        // Response records are deliberately untouched: deletion does not cascade
        // (D24, and upstream's own wording).
        Some(conversation)
    }

    pub fn insert_session(&mut self, row: SessionRow) {
        self.sessions_by_tenant
            .entry(row.session.tenant_id.clone())
            .or_default()
            .insert(row.session.id.clone());
        self.sessions.insert(row.session.id.clone(), row);
    }

    pub fn remove_session(&mut self, id: &SessionId) -> Option<SessionRow> {
        let row = self.sessions.remove(id)?;
        Self::unindex(&mut self.sessions_by_tenant, &row.session.tenant_id, id);
        Some(row)
    }

    /// Drop `id` from a tenant index, removing the tenant entry once empty so an
    /// index of empty sets cannot accumulate.
    fn unindex<K: Ord + Clone>(
        index: &mut HashMap<TenantId, BTreeSet<K>>,
        tenant: &TenantId,
        id: &K,
    ) {
        if let Some(set) = index.get_mut(tenant) {
            set.remove(id);
            if set.is_empty() {
                index.remove(tenant);
            }
        }
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
