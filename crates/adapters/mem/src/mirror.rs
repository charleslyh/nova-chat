//! In-process read-only Mirror view (V12 / FR-10).
//!
//! Public `StreamChannel` / `SnapshotStore` paths reject writes.
//! History is filled only via `project_*` (preserves source seq).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nova_sessions_core::{SessionId, SessionSnapshot, StreamEvent};
use nova_sessions_core::{SnapshotError, SnapshotStore, StreamChannel, StreamError, StreamGap};
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::stream::RECOVER_VIA_SNAPSHOT;

struct SessionMirrorLog {
    hot: Vec<StreamEvent>,
    cold: Vec<StreamEvent>,
    earliest: u64,
}

struct Inner {
    by_session: HashMap<SessionId, SessionMirrorLog>,
    snapshots: HashMap<SessionId, SessionSnapshot>,
}

pub struct MemMirrorView {
    inner: Mutex<Inner>,
    notify: Notify,
}

impl MemMirrorView {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                by_session: HashMap::new(),
                snapshots: HashMap::new(),
            }),
            notify: Notify::new(),
        }
    }

    fn gap(requested_from: u64, earliest_available: Option<u64>) -> StreamError {
        StreamError::Gap(StreamGap {
            requested_from,
            earliest_available,
            hint: RECOVER_VIA_SNAPSHOT.into(),
        })
    }

    /// Privilege path: project an authoritative event **keeping its seq**.
    pub fn project_event(&self, event: StreamEvent) {
        let mut g = self.inner.lock();
        let log = g.by_session.entry(event.session_id).or_insert_with(|| SessionMirrorLog {
            hot: Vec::new(),
            cold: Vec::new(),
            earliest: 1,
        });
        if event.seq < log.earliest {
            if !log.cold.iter().any(|e| e.seq == event.seq) {
                log.cold.push(event);
                log.cold.sort_by_key(|e| e.seq);
            }
            return;
        }
        if log.hot.iter().any(|e| e.seq == event.seq) {
            return;
        }
        log.hot.push(event);
        log.hot.sort_by_key(|e| e.seq);
        drop(g);
        self.notify.notify_waiters();
    }

    /// Privilege path: project snapshot (monotonic seq still enforced).
    pub fn project_snapshot(&self, snap: SessionSnapshot) -> Result<(), SnapshotError> {
        let mut g = self.inner.lock();
        if let Some(prev) = g.snapshots.get(&snap.session_id) {
            if snap.snapshot_seq < prev.snapshot_seq {
                return Err(SnapshotError::StaleSeq);
            }
        }
        g.snapshots.insert(snap.session_id, snap);
        Ok(())
    }

    /// Privilege path: mirror hot unload (INV-14 alignment with authority trim).
    pub fn project_trim(&self, session_id: SessionId, new_earliest: u64) {
        let mut g = self.inner.lock();
        if let Some(log) = g.by_session.get_mut(&session_id) {
            if new_earliest <= log.earliest {
                return;
            }
            let (stay, leave): (Vec<_>, Vec<_>) =
                log.hot.drain(..).partition(|e| e.seq >= new_earliest);
            log.cold.extend(leave);
            log.hot = stay;
            log.earliest = new_earliest;
        }
    }

    pub fn tip_seq(&self, session_id: SessionId) -> Option<u64> {
        let g = self.inner.lock();
        g.by_session
            .get(&session_id)
            .and_then(|l| l.hot.last().map(|e| e.seq))
    }
}

impl Default for MemMirrorView {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StreamChannel for MemMirrorView {
    async fn append(&self, _event: StreamEvent) -> Result<u64, StreamError> {
        Err(StreamError::ReadOnly)
    }

    async fn read_from(
        &self,
        session_id: SessionId,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<StreamEvent>, StreamError> {
        let g = self.inner.lock();
        let Some(log) = g.by_session.get(&session_id) else {
            if from_seq <= 1 {
                return Ok(vec![]);
            }
            return Err(Self::gap(from_seq, None));
        };
        if from_seq < log.earliest {
            return Err(Self::gap(from_seq, Some(log.earliest)));
        }
        Ok(log
            .hot
            .iter()
            .filter(|e| e.seq >= from_seq)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn read_after(
        &self,
        session_id: SessionId,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<StreamEvent>, StreamError> {
        for _ in 0..50 {
            {
                let g = self.inner.lock();
                if let Some(log) = g.by_session.get(&session_id) {
                    if after_seq + 1 < log.earliest && after_seq > 0 {
                        return Err(Self::gap(after_seq + 1, Some(log.earliest)));
                    }
                    let batch: Vec<_> = log
                        .hot
                        .iter()
                        .filter(|e| e.seq > after_seq)
                        .take(limit)
                        .cloned()
                        .collect();
                    if !batch.is_empty() {
                        return Ok(batch);
                    }
                }
            }
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }
        Ok(vec![])
    }
}

#[async_trait]
impl SnapshotStore for MemMirrorView {
    async fn put(&self, _snap: SessionSnapshot) -> Result<(), SnapshotError> {
        Err(SnapshotError::ReadOnly)
    }

    async fn get(&self, session_id: SessionId) -> Result<Option<SessionSnapshot>, SnapshotError> {
        Ok(self.inner.lock().snapshots.get(&session_id).cloned())
    }
}

/// Shared handle used by gateway / harness.
pub type SharedMirror = Arc<MemMirrorView>;

#[cfg(test)]
mod tests {
    use super::*;
    use nova_sessions_core::{EventKind, SessionId};

    #[tokio::test]
    async fn mirror_rejects_writes_and_keeps_source_seq() {
        let m = MemMirrorView::new();
        let sid = SessionId::new();
        assert!(matches!(
            m.append(StreamEvent {
                session_id: sid,
                seq: 0,
                kind: EventKind::TextDelta,
                turn_id: None,
                attempt: None,
                payload: "x".into(),
            })
            .await,
            Err(StreamError::ReadOnly)
        ));
        assert!(matches!(
            m.put(SessionSnapshot {
                session_id: sid,
                snapshot_seq: 1,
                bubbles: vec![],
                running: vec![],
            })
            .await,
            Err(SnapshotError::ReadOnly)
        ));

        m.project_event(StreamEvent {
            session_id: sid,
            seq: 3,
            kind: EventKind::TextDelta,
            turn_id: None,
            attempt: None,
            payload: "a".into(),
        });
        m.project_event(StreamEvent {
            session_id: sid,
            seq: 5,
            kind: EventKind::TextDelta,
            turn_id: None,
            attempt: None,
            payload: "b".into(),
        });
        let evs = m.read_from(sid, 3, 10).await.unwrap();
        assert_eq!(evs.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![3, 5]);
    }
}
