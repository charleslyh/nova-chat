use nova_responses_core::{ContextError, LedgerError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SqlError {
    #[error("database: {0}")]
    Db(#[from] sqlx::Error),
    #[error("migration: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("decode: {0}")]
    Decode(String),
    #[error("config: {0}")]
    Config(String),
}

/// Classify a driver error as "the store is unreachable" versus "the query was
/// wrong".
///
/// This distinction drives the refuse-writes degrade (INV-46): only genuine
/// unavailability may produce a 503. Mapping a logic error to `Unavailable`
/// would hide bugs behind a retryable status.
fn is_unavailable(err: &sqlx::Error) -> bool {
    matches!(
        err,
        sqlx::Error::PoolTimedOut
            | sqlx::Error::PoolClosed
            | sqlx::Error::Io(_)
            | sqlx::Error::Tls(_)
            | sqlx::Error::Configuration(_)
    )
}

pub(crate) fn to_context_error(err: sqlx::Error) -> ContextError {
    if is_unavailable(&err) {
        ContextError::Unavailable
    } else {
        ContextError::Internal(err.to_string())
    }
}

pub(crate) fn to_ledger_error(err: sqlx::Error) -> LedgerError {
    if is_unavailable(&err) {
        LedgerError::Unavailable
    } else {
        LedgerError::Internal(err.to_string())
    }
}

/// Whether a driver error is a unique-constraint violation, used to detect an
/// idempotency-key collision without a pre-read.
pub(crate) fn is_unique_violation(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db) => db.code().as_deref() == Some("23505"),
        _ => false,
    }
}
