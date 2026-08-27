use std::collections::HashMap;

use async_trait::async_trait;
use nova_sessions_core::{SessionId, SessionSnapshot};
use nova_sessions_core::{SnapshotError, SnapshotStore};
use parking_lot::Mutex;

pub struct MemSnapshotStore {
    inner: Mutex<HashMap<SessionId, SessionSnapshot>>,
}

impl MemSnapshotStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for MemSnapshotStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SnapshotStore for MemSnapshotStore {
    async fn put(&self, snap: SessionSnapshot) -> Result<(), SnapshotError> {
        let mut g = self.inner.lock();
        if let Some(prev) = g.get(&snap.session_id) {
            if snap.snapshot_seq < prev.snapshot_seq {
                return Err(SnapshotError::StaleSeq);
            }
        }
        g.insert(snap.session_id, snap);
        Ok(())
    }

    async fn get(&self, session_id: SessionId) -> Result<Option<SessionSnapshot>, SnapshotError> {
        Ok(self.inner.lock().get(&session_id).cloned())
    }
}
