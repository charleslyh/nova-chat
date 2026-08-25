use serde::{Deserialize, Serialize};

use crate::error::DomainError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Pending,
    Claimed,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    DeadLetter,
}

impl TaskState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TaskState::Succeeded | TaskState::Failed | TaskState::Cancelled | TaskState::DeadLetter
        )
    }
}

/// Pure transition function. Every non-terminal state must have a timeout exit elsewhere (INV-34/35).
pub fn transition(from: TaskState, to: TaskState) -> Result<(), DomainError> {
    let ok = matches!(
        (from, to),
        (TaskState::Pending, TaskState::Claimed)
            | (TaskState::Pending, TaskState::Cancelled)
            | (TaskState::Pending, TaskState::DeadLetter)
            | (TaskState::Claimed, TaskState::Running)
            | (TaskState::Claimed, TaskState::Pending) // reclaim
            | (TaskState::Claimed, TaskState::Failed)
            | (TaskState::Running, TaskState::Succeeded)
            | (TaskState::Running, TaskState::Failed)
            | (TaskState::Running, TaskState::Pending) // reclaim
            | (TaskState::Failed, TaskState::Pending) // retry
            | (TaskState::Failed, TaskState::DeadLetter)
    );
    if ok {
        Ok(())
    } else {
        Err(DomainError::InvalidTransition { from, to })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_to_claimed_ok() {
        assert!(transition(TaskState::Pending, TaskState::Claimed).is_ok());
    }

    #[test]
    fn succeeded_has_no_exit() {
        assert!(TaskState::Succeeded.is_terminal());
        assert!(transition(TaskState::Succeeded, TaskState::Pending).is_err());
    }
}
