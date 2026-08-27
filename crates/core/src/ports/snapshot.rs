use async_trait::async_trait;
use crate::{SessionId, SessionSnapshot};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("stale snapshot_seq")]
    StaleSeq,
    #[error("read only")]
    ReadOnly,
    #[error("internal: {0}")]
    Internal(String),
}

#[async_trait]
pub trait SnapshotStore: Send + Sync {
    async fn put(&self, snap: SessionSnapshot) -> Result<(), SnapshotError>;
    async fn get(&self, session_id: SessionId) -> Result<Option<SessionSnapshot>, SnapshotError>;
}
