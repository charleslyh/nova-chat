use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nova_sessions_core::{SessionId, StreamEvent};
use nova_sessions_core::{MetaStore, StreamChannel, StreamError, StreamGap};
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::meta::MemMetaStore;

struct SessionLog {
    events: Vec<StreamEvent>,
    /// Lowest seq still present (1-based). After trim would rise; mem never trims in v1.
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
}

#[async_trait]
impl StreamChannel for MemStreamChannel {
    async fn append(&self, mut event: StreamEvent) -> Result<u64, StreamError> {
        if let (Some(tid), Some(att)) = (event.turn_id, event.attempt) {
            self.meta
                .check_attempt(tid, att)
                .await
                .map_err(|e| match e {
                    nova_sessions_core::MetaError::StaleAttempt => StreamError::StaleAttempt,
                    other => StreamError::Internal(other.to_string()),
                })?;
        }

        let mut g = self.inner.lock();
        let log = g.by_session.entry(event.session_id).or_insert_with(|| SessionLog {
            events: Vec::new(),
            earliest: 1,
        });
        let seq = log.events.len() as u64 + log.earliest;
        event.seq = seq;
        log.events.push(event);
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
            return Err(StreamError::Gap(StreamGap {
                requested_from: from_seq,
                earliest_available: None,
                hint: "recover_via_snapshot".into(),
            }));
        };
        if from_seq < log.earliest {
            return Err(StreamError::Gap(StreamGap {
                requested_from: from_seq,
                earliest_available: Some(log.earliest),
                hint: "recover_via_snapshot".into(),
            }));
        }
        // from_seq is inclusive for resume "events with seq >= from_seq"
        // Callers using after_seq exclusive should use read_after.
        Ok(log
            .events
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
        // Poll with notify for up to a short wait.
        for _ in 0..50 {
            {
                let g = self.inner.lock();
                if let Some(log) = g.by_session.get(&session_id) {
                    if after_seq + 1 < log.earliest && after_seq > 0 {
                        return Err(StreamError::Gap(StreamGap {
                            requested_from: after_seq + 1,
                            earliest_available: Some(log.earliest),
                            hint: "recover_via_snapshot".into(),
                        }));
                    }
                    let batch: Vec<_> = log
                        .events
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
    /// Test-only: simulate hot-layer unload (INV-14). Raises `earliest` and drops older events.
    pub fn test_trim_earliest(&self, session_id: SessionId, new_earliest: u64) {
        let mut g = self.inner.lock();
        if let Some(log) = g.by_session.get_mut(&session_id) {
            log.events.retain(|e| e.seq >= new_earliest);
            if new_earliest > log.earliest {
                log.earliest = new_earliest;
            }
        }
    }
}
