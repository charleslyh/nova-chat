use async_trait::async_trait;
use nova_core::{TaskSpec, WorkerProfile};
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct SandboxInput {
    pub task: TaskSpec,
    pub worker: WorkerProfile,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SandboxError {
    #[error("budget exceeded")]
    BudgetExceeded,
    #[error("forbidden capability")]
    Forbidden,
    #[error("eval error: {0}")]
    Eval(String),
}

/// INV-18/19 / SEC-6: projected inputs only; bounded execution.
#[async_trait]
pub trait PolicySandbox: Send + Sync {
    async fn eval(
        &self,
        program: &str,
        input: &SandboxInput,
        instruction_budget: u64,
    ) -> Result<bool, SandboxError>;
}
