use async_trait::async_trait;
use nova_ports::{PolicySandbox, SandboxError, SandboxInput};

/// Restricted interpreter: closed ops only.
/// Programs:
/// - empty / "true" => true
/// - "false" => false
/// - "capacity_le:<n>" => worker.remaining_capacity >= task.capacity && task.capacity.units <= n
/// - "label:<name>" => worker has label
pub struct MemPolicySandbox;

impl MemPolicySandbox {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MemPolicySandbox {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PolicySandbox for MemPolicySandbox {
    async fn eval(
        &self,
        program: &str,
        input: &SandboxInput,
        instruction_budget: u64,
    ) -> Result<bool, SandboxError> {
        if instruction_budget == 0 {
            return Err(SandboxError::BudgetExceeded);
        }
        let p = program.trim();
        if p.is_empty() || p == "true" {
            return Ok(true);
        }
        if p == "false" {
            return Ok(false);
        }
        if let Some(rest) = p.strip_prefix("capacity_le:") {
            let n: u32 = rest
                .parse()
                .map_err(|e| SandboxError::Eval(format!("bad int: {e}")))?;
            let ok = input.task.capacity.units <= n
                && input.worker.remaining_capacity >= input.task.capacity.units;
            return Ok(ok);
        }
        if let Some(label) = p.strip_prefix("label:") {
            return Ok(input.worker.labels.iter().any(|l| l == label));
        }
        if p.contains("file://") || p.contains("http://") || p.contains("net.") {
            return Err(SandboxError::Forbidden);
        }
        Err(SandboxError::Eval(format!("unknown program: {p}")))
    }
}
