use async_trait::async_trait;
use nova_core::WorkerId;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LedgerError {
    #[error("insufficient capacity")]
    Insufficient,
    #[error("internal: {0}")]
    Internal(String),
}

/// Server-side capacity ledger (D10 / INV-3 / INV-4). Does not trust device self-report for remaining.
#[async_trait]
pub trait CapacityLedger: Send + Sync {
    async fn register_worker(&self, worker: WorkerId, total: u32) -> Result<(), LedgerError>;
    async fn remaining(&self, worker: &WorkerId) -> Result<u32, LedgerError>;
    async fn reserve(&self, worker: &WorkerId, units: u32) -> Result<(), LedgerError>;
    async fn release(&self, worker: &WorkerId, units: u32) -> Result<(), LedgerError>;
}
