use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nova_sessions_core::{SessionId, StreamEvent};
use nova_sessions_core::{MetaStore, StreamChannel, StreamError, StreamGap};
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::meta::MemMetaStore;

/// Stable Gap recover hint (FR-13 / INV-14). Clients: GET snapshot → SSE from snapshot_seq.
pub const RECOVER_VIA_SNAPSHOT: &str = "recover_via_snapshot";

struct SessionLog {
    /// Hot layer: contiguous events with `seq >= earliest`.
    hot: Vec<StreamEvent>,
    /// Cold segment: archived on trim; not served by `read_from` (Gap → snapshot path).
    cold: Vec<StreamEvent>,
    /// Lowest seq still in hot (1-based).
    earliest: u64,
}

struct Inner {
    by_session: HashMap<SessionId, SessionLog>,
}

pub struct MemStreamChannel {
    inner: Mutex<Inner>,
    notify: Notify,
    meta: Arc<MemMetaStore>,
}

impl MemStreamChannel {
    pub fn new(meta: Arc<MemMetaStore>) -> Self {
        Self {
            inner: Mutex::new(Inner {
                by_session: HashMap::new(),
            }),
            notify: Notify::new(),
            meta,
        }
    }

    fn gap(requested_from: u64, earliest_available: Option<u64>) -> StreamError {
        StreamError::Gap(StreamGap {
            requested_from,
            earliest_available,
            hint: RECOVER_VIA_SNAPSHOT.into(),
        })
    }
}

#[async_trait]
impl StreamChannel for MemStreamChannel {
    async fn append(&self, mut event: StreamEvent) -> Result<u64, StreamError> {
        if self.meta.is_read_only() {
            return Err(StreamError::ReadOnly);
        }
        if let (Some(tid), Some(att)) = (event.turn_id, event.attempt) {
            self.meta
                .check_attempt(tid, att)
                .await
                .map_err(|e| match e {
                    nova_sessions_core::MetaError::StaleAttempt => StreamError::StaleAttempt,
                    nova_sessions_core::MetaError::ReadOnly => StreamError::ReadOnly,
                    other => StreamError::Internal(other.to_string()),
                })?;
        }

        let mut g = self.inner.lock();
        let log = g.by_session.entry(event.session_id).or_insert_with(|| SessionLog {
            hot: Vec::new(),
            cold: Vec::new(),
            earliest: 1,
        });
        let seq = log.hot.len() as u64 + log.earliest;
        event.seq = seq;
        log.hot.push(event);
        drop(g);
        self.notify.notify_waiters();
        Ok(seq)
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
        // from_seq is inclusive for resume "events with seq >= from_seq"
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
                } else if after_seq > 0 {
                    // Session not yet written — wait for first events.
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

impl MemStreamChannel {
    /// Simulate hot-layer unload (INV-14 / FR-14): archive `[earliest, new_earliest)` into cold, then raise earliest.
    /// Hot `read_from` below `new_earliest` returns Gap with `recover_via_snapshot` — cold is not silently spliced.
    pub fn trim_earliest(&self, session_id: SessionId, new_earliest: u64) {
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

    /// Test hook alias.
    pub fn test_trim_earliest(&self, session_id: SessionId, new_earliest: u64) {
        self.trim_earliest(session_id, new_earliest);
    }

    /// Events retained in cold after trims (FR-14: history not discarded).
    pub fn cold_len(&self, session_id: SessionId) -> usize {
        let g = self.inner.lock();
        g.by_session
            .get(&session_id)
            .map(|l| l.cold.len())
            .unwrap_or(0)
    }

    /// Tip seq in hot (for snapshot refresh without `read_from(1, …)`).
    pub fn tip_seq(&self, session_id: SessionId) -> Option<u64> {
        let g = self.inner.lock();
        g.by_session
            .get(&session_id)
            .and_then(|l| l.hot.last().map(|e| e.seq))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::MemMetaStore;
    use nova_sessions_core::{EventKind, SessionId};

    #[tokio::test]
    async fn trim_archives_to_cold_and_gaps() {
        let meta = Arc::new(MemMetaStore::new());
        let stream = MemStreamChannel::new(meta);
        let sid = SessionId::new();
        for i in 0..5 {
            stream
                .append(StreamEvent {
                    session_id: sid,
                    seq: 0,
                    kind: EventKind::TextDelta,
                    turn_id: None,
                    attempt: None,
                    payload: format!("{i}"),
                })
                .await
                .unwrap();
        }
        stream.trim_earliest(sid, 3);
        assert_eq!(stream.cold_len(sid), 2);
        assert!(matches!(
            stream.read_from(sid, 1, 10).await,
            Err(StreamError::Gap(g)) if g.hint == RECOVER_VIA_SNAPSHOT && g.earliest_available == Some(3)
        ));
        let hot = stream.read_from(sid, 3, 10).await.unwrap();
        assert_eq!(hot.len(), 3);
        assert_eq!(hot[0].seq, 3);
    }
}
