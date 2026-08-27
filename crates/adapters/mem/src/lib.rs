//! In-memory adapters for Session stream stack (D19).

mod meta;
mod stream;
mod snapshot;
mod mirror;
mod clock;
mod metrics;

pub use clock::MemClock;
pub use meta::MemMetaStore;
pub use metrics::MemMetrics;
pub use mirror::{MemMirrorView, SharedMirror};
pub use snapshot::MemSnapshotStore;
pub use stream::{MemStreamChannel, RECOVER_VIA_SNAPSHOT};

use std::sync::Arc;

#[derive(Clone)]
pub struct MemWorld {
    pub meta: Arc<MemMetaStore>,
    pub stream: Arc<MemStreamChannel>,
    pub snapshot: Arc<MemSnapshotStore>,
    /// In-process read-only projection (V12 / FR-10).
    pub mirror: Arc<MemMirrorView>,
    pub clock: Arc<MemClock>,
    pub metrics: Arc<MemMetrics>,
}

impl MemWorld {
    pub fn new() -> Self {
        let meta = Arc::new(MemMetaStore::new());
        let stream = Arc::new(MemStreamChannel::new(meta.clone()));
        Self {
            meta,
            stream,
            snapshot: Arc::new(MemSnapshotStore::new()),
            mirror: Arc::new(MemMirrorView::new()),
            clock: Arc::new(MemClock::new()),
            metrics: Arc::new(MemMetrics::new()),
        }
    }

    /// Sync-project authority stream+snapshot into mirror (test / home helper).
    pub async fn sync_mirror(&self, session_id: nova_sessions_core::SessionId) {
        use nova_sessions_core::{SnapshotStore, StreamChannel, StreamError};
        let from = match self.stream.read_from(session_id, 1, 10_000).await {
            Ok(evs) => {
                for e in evs {
                    self.mirror.project_event(e);
                }
                None
            }
            Err(StreamError::Gap(g)) => g.earliest_available.or(Some(1)),
            Err(_) => None,
        };
        if let Some(from) = from {
            if let Ok(evs) = self.stream.read_from(session_id, from, 10_000).await {
                for e in evs {
                    self.mirror.project_event(e);
                }
            }
        }
        if let Ok(Some(s)) = self.snapshot.get(session_id).await {
            let _ = self.mirror.project_snapshot(s);
        }
    }
}

impl Default for MemWorld {
    fn default() -> Self {
        Self::new()
    }
}
