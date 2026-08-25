use async_trait::async_trait;
use nova_core::IdempotencyKey;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reservation {
    Reserved,
    AlreadyExists,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum GateError {
    #[error("internal: {0}")]
    Internal(String),
}

/// INV-2: existence rejects; signature has no TTL.
#[async_trait]
pub trait IdempotencyGate: Send + Sync {
    async fn reserve(&self, key: &IdempotencyKey) -> Result<Reservation, GateError>;
}
