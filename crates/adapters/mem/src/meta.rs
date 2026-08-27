use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use nova_sessions_core::{AgentId, Attempt, IdempotencyKey, SessionId, TurnId};
use nova_sessions_core::{
    ClaimedTurn, MetaError, MetaStore, SessionLock, SubmitOutcome, TurnRecord, TurnStatus,
};
use parking_lot::Mutex;

struct SessionRow {
    lock: SessionLock,
}

struct Inner {
    sessions: HashMap<SessionId, SessionRow>,
    turns: HashMap<TurnId, TurnRecord>,
    pending: VecDeque<TurnId>,
    /// idempotency_key -> turn_id
    idem: HashMap<String, TurnId>,
    /// agent_id -> last heartbeat ms
    heartbeats: HashMap<AgentId, u64>,
}

pub struct MemMetaStore {
    inner: Mutex<Inner>,
    read_only: AtomicBool,
    /// FR-18: max Pending+Claimed turns; reject with Overloaded when at limit.
    pending_limit: AtomicUsize,
}

impl MemMetaStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                sessions: HashMap::new(),
                turns: HashMap::new(),
                pending: VecDeque::new(),
                idem: HashMap::new(),
                heartbeats: HashMap::new(),
            }),
            read_only: AtomicBool::new(false),
            pending_limit: AtomicUsize::new(10_000),
        }
    }

    /// INV-32: toggle read-only degrade (writes rejected, reads still allowed).
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

    fn guard_writable(&self) -> Result<(), MetaError> {
        if self.is_read_only() {
            Err(MetaError::ReadOnly)
        } else {
            Ok(())
        }
    }

    fn inflight_locked(g: &Inner) -> usize {
        g.turns
            .values()
            .filter(|t| matches!(t.status, TurnStatus::Pending | TurnStatus::Claimed))
            .count()
    }
}

impl Default for MemMetaStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MetaStore for MemMetaStore {
    async fn create_session(&self) -> Result<SessionId, MetaError> {
        let id = SessionId::new();
        self.inner.lock().sessions.insert(
            id,
            SessionRow {
                lock: SessionLock::Idle,
            },
        );
        Ok(id)
    }

    async fn submit_turn(
        &self,
        session_id: SessionId,
        text: String,
        idempotency_key: IdempotencyKey,
        _now_ms: u64,
    ) -> Result<SubmitOutcome, MetaError> {
        // Idempotent replay of an already-accepted key still returns Duplicate even in read-only.
        {
            let g = self.inner.lock();
            if let Some(tid) = g.idem.get(&idempotency_key.0) {
                return Ok(SubmitOutcome::Duplicate { turn_id: *tid });
            }
        }
        if self.is_read_only() {
            return Ok(SubmitOutcome::ReadOnly);
        }
        let mut g = self.inner.lock();
        let limit = self.pending_limit();
        if Self::inflight_locked(&g) >= limit {
            return Ok(SubmitOutcome::Overloaded);
        }
        let sess = g.sessions.get_mut(&session_id).ok_or(MetaError::NotFound)?;
        if matches!(sess.lock, SessionLock::Busy) {
            return Ok(SubmitOutcome::Busy);
        }
        sess.lock = SessionLock::Busy;
        let turn_id = TurnId::new();
        let rec = TurnRecord {
            turn_id,
            session_id,
            text,
            status: TurnStatus::Pending,
            attempt: Attempt(0),
            owner: None,
            exec_deadline_ms: None,
        };
        g.turns.insert(turn_id, rec);
        g.pending.push_back(turn_id);
        g.idem.insert(idempotency_key.0, turn_id);
        Ok(SubmitOutcome::Accepted { turn_id })
    }

    async fn claim_turn(
        &self,
        agent_id: AgentId,
        now_ms: u64,
        exec_ttl_ms: u64,
    ) -> Result<Option<ClaimedTurn>, MetaError> {
        self.guard_writable()?;
        let mut g = self.inner.lock();
        let Some(turn_id) = g.pending.pop_front() else {
            return Ok(None);
        };
        let Some(rec) = g.turns.get_mut(&turn_id) else {
            return Ok(None);
        };
        if rec.status != TurnStatus::Pending {
            return Ok(None);
        }
        let attempt = rec.attempt.next();
        rec.attempt = attempt;
        rec.status = TurnStatus::Claimed;
        rec.owner = Some(agent_id);
        let deadline = now_ms.saturating_add(exec_ttl_ms);
        rec.exec_deadline_ms = Some(deadline);
        let turn = rec.clone();
        g.heartbeats.insert(agent_id, now_ms);
        Ok(Some(ClaimedTurn {
            turn,
            attempt,
            exec_deadline_ms: deadline,
        }))
    }

    async fn complete_turn(
        &self,
        turn_id: TurnId,
        expected_attempt: Attempt,
        to: TurnStatus,
    ) -> Result<(), MetaError> {
        self.guard_writable()?;
        let mut g = self.inner.lock();
        let Some(rec) = g.turns.get_mut(&turn_id) else {
            return Err(MetaError::NotFound);
        };
        if rec.attempt != expected_attempt {
            return Err(MetaError::StaleAttempt);
        }
        if rec.status != TurnStatus::Claimed {
            return Err(MetaError::StaleAttempt);
        }
        if !matches!(to, TurnStatus::Done | TurnStatus::Failed) {
            return Err(MetaError::Internal("invalid complete status".into()));
        }
        let session_id = rec.session_id;
        rec.status = to;
        rec.owner = None;
        rec.exec_deadline_ms = None;
        if let Some(sess) = g.sessions.get_mut(&session_id) {
            sess.lock = SessionLock::Idle;
        }
        Ok(())
    }

    async fn heartbeat(&self, agent_id: AgentId, now_ms: u64) -> Result<(), MetaError> {
        self.inner.lock().heartbeats.insert(agent_id, now_ms);
        Ok(())
    }

    async fn reap(
        &self,
        now_ms: u64,
        heartbeat_ttl_ms: u64,
    ) -> Result<Vec<(TurnId, Attempt, SessionId)>, MetaError> {
        let mut g = self.inner.lock();
        let mut aborted = Vec::new();
        let mut to_fail: Vec<TurnId> = Vec::new();

        for (tid, rec) in g.turns.iter() {
            if rec.status != TurnStatus::Claimed {
                continue;
            }
            let timed_out = rec
                .exec_deadline_ms
                .map(|d| now_ms >= d)
                .unwrap_or(false);
            let lost = rec
                .owner
                .and_then(|a| g.heartbeats.get(&a).copied())
                .map(|hb| now_ms.saturating_sub(hb) > heartbeat_ttl_ms)
                .unwrap_or(true);
            if timed_out || lost {
                to_fail.push(*tid);
            }
        }

        for tid in to_fail {
            if let Some(rec) = g.turns.get_mut(&tid) {
                let old = rec.attempt;
                let sid = rec.session_id;
                // Raise attempt fence so stale writer cannot append.
                rec.attempt = old.next();
                rec.status = TurnStatus::Failed;
                rec.owner = None;
                rec.exec_deadline_ms = None;
                if let Some(sess) = g.sessions.get_mut(&sid) {
                    sess.lock = SessionLock::Idle;
                }
                aborted.push((tid, old, sid));
            }
        }
        Ok(aborted)
    }

    async fn get_turn(&self, turn_id: TurnId) -> Result<Option<TurnRecord>, MetaError> {
        Ok(self.inner.lock().turns.get(&turn_id).cloned())
    }

    async fn lock(&self, session_id: SessionId) -> Result<SessionLock, MetaError> {
        self.inner
            .lock()
            .sessions
            .get(&session_id)
            .map(|s| s.lock)
            .ok_or(MetaError::NotFound)
    }

    async fn check_attempt(&self, turn_id: TurnId, attempt: Attempt) -> Result<(), MetaError> {
        self.guard_writable()?;
        let g = self.inner.lock();
        let Some(rec) = g.turns.get(&turn_id) else {
            return Err(MetaError::NotFound);
        };
        if rec.attempt != attempt {
            return Err(MetaError::StaleAttempt);
        }
        if rec.status != TurnStatus::Claimed {
            return Err(MetaError::StaleAttempt);
        }
        Ok(())
    }
}

/// Shared Arc helper for stream fence (kept for callers).
#[allow(dead_code)]
pub type SharedMeta = Arc<MemMetaStore>;
