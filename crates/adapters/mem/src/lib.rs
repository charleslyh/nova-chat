//! In-memory adapters for Session stream stack (D19).

mod meta;
mod stream;
mod snapshot;
mod clock;
mod metrics;

pub use clock::MemClock;
pub use meta::MemMetaStore;
pub use metrics::MemMetrics;
pub use snapshot::MemSnapshotStore;
pub use stream::MemStreamChannel;

use std::sync::Arc;

#[derive(Clone)]
pub struct MemWorld {
    pub meta: Arc<MemMetaStore>,
    pub stream: Arc<MemStreamChannel>,
    pub snapshot: Arc<MemSnapshotStore>,
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
            clock: Arc::new(MemClock::new()),
            metrics: Arc::new(MemMetrics::new()),
        }
    }
}

impl Default for MemWorld {
    fn default() -> Self {
        Self::new()
    }
}
