use thiserror::Error;

use crate::state::TaskState;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DomainError {
    #[error("invalid transition {from:?} -> {to:?}")]
    InvalidTransition { from: TaskState, to: TaskState },
}
