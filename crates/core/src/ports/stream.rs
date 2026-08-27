use async_trait::async_trait;
use crate::{SessionId, StreamEvent};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamGap {
    pub requested_from: u64,
    pub earliest_available: Option<u64>,
    pub hint: String,
}

#[derive(Debug, Error)]
pub enum StreamError {
    #[error("gap: {0:?}")]
    Gap(StreamGap),
    #[error("stale attempt")]
    StaleAttempt,
    #[error("read only")]
    ReadOnly,
    #[error("internal: {0}")]
    Internal(String),
}

/// D11 / INV-11: path separated from MetaStore; seq allocated inside the channel.
#[async_trait]
pub trait StreamChannel: Send + Sync {
    async fn append(&self, event: StreamEvent) -> Result<u64, StreamError>;

    /// INV-14: if hot layer does not contain `from_seq`, return Gap — never silent hole-fill.
    async fn read_from(
        &self,
        session_id: SessionId,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<StreamEvent>, StreamError>;

    /// Subscribe-style: wait for next events after `after_seq` (exclusive), with timeout.
    async fn read_after(
        &self,
        session_id: SessionId,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<StreamEvent>, StreamError>;
}
