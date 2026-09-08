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
    AgentId, Attempt, Conversation, ConversationEvent, ConversationEventKind, ConversationId,
    ResponseId, StoredResponse, TenantId, Usage,
};
use parking_lot::{Mutex, MutexGuard};
use tokio::sync::Notify;

/// A conversation's event stream (D28). Held beside the conversation under the
/// same mutex so the in-flight marker and the turn boundary events land together.
#[derive(Default)]
pub(crate) struct ConversationEventStream {
    pub next_seq: u64,
    pub events: Vec<ConversationEvent>,
    /// Sequence of the current turn's `TurnStarted`, for re-entrant `acquire_active`.
    pub lock_seq: Option<u64>,
    /// Last completed turn and its `TurnCompleted` seq, for idempotent `release_active`.
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
    /// items of their own (D27), plus the in-flight marker (D28).
    pub conversations: HashMap<ConversationId, Conversation>,
    pub conversations_by_tenant: HashMap<TenantId, BTreeSet<ConversationId>>,

    /// Per-conversation event streams (D28). Kept beside `conversations` under the
    /// same mutex, so a turn boundary (marker transition + event) and the
    /// response it refers to cannot be observed out of step.
    pub conversation_events: HashMap<ConversationId, ConversationEventStream>,
}

pub struct MemStore {
    inner: Mutex<Inner>,
    read_only: AtomicBool,
    unavailable: AtomicBool,
    pending_limit: AtomicUsize,
    max_records: AtomicUsize,
    max_conversations: AtomicUsize,
    max_events_per_conversation: AtomicUsize,
    /// Wakes conversation-event subscribers (the SSE `read_after` long poll) when
    /// a new event is appended. Without it the poll would have to busy-wait, which
    /// is exactly the CPU-100% hot loop this prevents.
    conversation_notify: Notify,
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
                conversation_events: HashMap::new(),
            }),
            read_only: AtomicBool::new(false),
            unavailable: AtomicBool::new(false),
            pending_limit: AtomicUsize::new(10_000),
            max_records: AtomicUsize::new(100_000),
            max_conversations: AtomicUsize::new(100_000),
            max_events_per_conversation: AtomicUsize::new(100_000),
            conversation_notify: Notify::new(),
        }
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock()
    }

    /// Wake every conversation-event subscriber after an append. Non-blocking, so
    /// it is safe to call while the store mutex is still held.
    pub(crate) fn notify_conversation_event(&self) {
        self.conversation_notify.notify_waiters();
    }

    /// Wait for the next conversation-event append (spurious wakeups allowed).
    pub(crate) async fn wait_conversation_event(&self) {
        self.conversation_notify.notified().await;
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

    /// Upper bound on one conversation's event stream (D28).
    ///
    /// Reaching it **refuses the append** rather than evicting the oldest events,
    /// unlike the per-response event ring: dropping a turn boundary or a business
    /// event loses the only record that it happened.
    pub fn set_max_events_per_conversation(&self, limit: usize) {
        self.max_events_per_conversation
            .store(limit.max(1), Ordering::SeqCst);
    }

    pub fn max_events_per_conversation(&self) -> usize {
        self.max_events_per_conversation.load(Ordering::SeqCst)
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
        // The event stream goes with it; response records are deliberately
        // untouched (D24, and upstream's own wording).
        self.conversation_events.remove(id);
        Some(conversation)
    }

    /// Append a conversation event and return its sequence number. Caller holds
    /// the mutex, so allocation is atomic with any marker transition it is paired
    /// with.
    pub fn push_conversation_event(
        &mut self,
        id: &ConversationId,
        kind: ConversationEventKind,
        now_ms: u64,
    ) -> u64 {
        let stream = self.conversation_events.entry(id.clone()).or_default();
        let seq = stream.next_seq;
        stream.next_seq += 1;
        stream.events.push(ConversationEvent {
            conversation_id: id.clone(),
            seq,
            kind,
            ts_ms: now_ms,
        });
        seq
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
