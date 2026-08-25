use async_trait::async_trait;
use nova_core::OutputEvent;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StreamError {
    #[error("internal: {0}")]
    Internal(String),
}

/// D11 / INV-11: path separated from TaskStore; seq allocated inside the channel.
#[async_trait]
pub trait StreamChannel: Send + Sync {
    async fn append(&self, event: OutputEvent) -> Result<u64, StreamError>;
    async fn read_from(
        &self,
        task_id: nova_core::TaskId,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<OutputEvent>, StreamError>;
}
