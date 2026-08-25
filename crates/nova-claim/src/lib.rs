//! Claim protocol orchestration (design/01). Persistence via ports only.

use std::sync::Arc;

use nova_core::{Attempt, TaskId, TaskSpec, TaskState, WorkerId, WorkerProfile};
use nova_matcher::Matcher;
use nova_ports::{
    CapacityAssertion, CapacityLedger, ClaimOutcome, Clock, PolicySandbox, TaskStore,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClaimError {
    #[error("no eligible task")]
    NoEligible,
    #[error("store: {0}")]
    Store(String),
    #[error("ledger: {0}")]
    Ledger(String),
    #[error("backpressure")]
    Backpressure,
}

pub struct ClaimService {
    store: Arc<dyn TaskStore>,
    ledger: Arc<dyn CapacityLedger>,
    sandbox: Arc<dyn PolicySandbox>,
    clock: Arc<dyn Clock>,
    matcher: Matcher,
    pending_threshold: usize,
}

impl ClaimService {
    pub fn new(
        store: Arc<dyn TaskStore>,
        ledger: Arc<dyn CapacityLedger>,
        sandbox: Arc<dyn PolicySandbox>,
        clock: Arc<dyn Clock>,
        matcher: Matcher,
        pending_threshold: usize,
    ) -> Self {
        Self {
            store,
            ledger,
            sandbox,
            clock,
            matcher,
            pending_threshold,
        }
    }

    pub async fn submit_allowed(&self) -> Result<(), ClaimError> {
        let pending = self
            .store
            .list_candidates(&nova_ports::FilterExpr {
                pending_only: true,
                limit: self.pending_threshold + 1,
            })
            .await
            .map_err(|e| ClaimError::Store(e.to_string()))?;
        if pending.len() >= self.pending_threshold {
            return Err(ClaimError::Backpressure);
        }
        Ok(())
    }

    /// Three-phase claim: filter → eligible → atomic try_claim (+ ledger reserve).
    pub async fn claim_one(
        &self,
        worker: &WorkerProfile,
    ) -> Result<(TaskSpec, Attempt), ClaimError> {
        let remaining = self
            .ledger
            .remaining(&worker.id)
            .await
            .map_err(|e| ClaimError::Ledger(e.to_string()))?;
        let profile = WorkerProfile {
            remaining_capacity: remaining,
            ..worker.clone()
        };

        let filter = self.matcher.coarse_filter(&profile, 100);
        let mut candidates = self
            .store
            .list_candidates(&filter)
            .await
            .map_err(|e| ClaimError::Store(e.to_string()))?;
        let now = self.clock.now_ms().await;
        candidates.sort_by_key(|t| self.matcher.rank(t, now));

        for task in candidates {
            if !self.matcher.eligible(self.sandbox.as_ref(), &task, &profile).await {
                continue;
            }
            let rec = self
                .store
                .get(&task.id)
                .await
                .map_err(|e| ClaimError::Store(e.to_string()))?
                .ok_or_else(|| ClaimError::Store("missing".into()))?;
            if rec.state != TaskState::Pending {
                continue;
            }
            if self
                .ledger
                .reserve(&worker.id, task.capacity.units)
                .await
                .is_err()
            {
                continue;
            }
            let deadline = now + task.kind.exec_deadline_secs() * 1000;
            let outcome = self
                .store
                .try_claim(
                    &task.id,
                    TaskState::Pending,
                    rec.attempt,
                    &worker.id,
                    &CapacityAssertion {
                        required: task.capacity.units,
                    },
                    deadline,
                )
                .await
                .map_err(|e| ClaimError::Store(e.to_string()))?;

            match outcome {
                ClaimOutcome::Claimed { attempt } => return Ok((task, attempt)),
                _ => {
                    let _ = self.ledger.release(&worker.id, task.capacity.units).await;
                }
            }
        }
        Err(ClaimError::NoEligible)
    }

    pub async fn complete(
        &self,
        task_id: &TaskId,
        worker: &WorkerId,
        attempt: Attempt,
        units: u32,
        success: bool,
    ) -> Result<(), ClaimError> {
        let to = if success {
            TaskState::Succeeded
        } else {
            TaskState::Failed
        };
        let ok = self
            .store
            .finish(task_id, attempt, to)
            .await
            .map_err(|e| ClaimError::Store(e.to_string()))?;
        if ok {
            let _ = self.ledger.release(worker, units).await;
        }
        Ok(())
    }

    pub async fn reclaim(
        &self,
        task_id: &TaskId,
        attempt: Attempt,
        worker: &WorkerId,
        units: u32,
    ) -> Result<(), ClaimError> {
        let ok = self
            .store
            .release_to_pending(task_id, attempt)
            .await
            .map_err(|e| ClaimError::Store(e.to_string()))?;
        if ok {
            let _ = self.ledger.release(worker, units).await;
        }
        Ok(())
    }
}
