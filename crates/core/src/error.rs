use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DomainError {
    #[error("invalid transition: {0}")]
    InvalidTransition(String),
    #[error("busy")]
    Busy,
    #[error("not found")]
    NotFound,
    #[error("stale attempt")]
    StaleAttempt,
    #[error("duplicate")]
    Duplicate,
}
