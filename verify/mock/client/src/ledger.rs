//! Ledger client: the per-node admission controls live here (read_only,
//! pending_limit); the shared data operations are forwarded to the carrier.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nova_responses::{AgentId, Attempt, IdempotencyKey, ResponseId, ResponseRecord, ResponseStatus, TenantId, Usage};
use nova_responses::ports::{
    AbortedClaim, AdmissionControl, ClaimedResponse, CreateOutcome, LedgerError, ResponseLedger,
    StoreError,
};

use mock_server::proto::{ProtoError, Request, Response};

use crate::rpc::Rpc;

pub struct MemLedgerClient {
    rpc: Arc<Rpc>,
    read_only: Arc<AtomicBool>,
    pending_limit: Arc<AtomicUsize>,
}

impl MemLedgerClient {
    pub fn new(rpc: Arc<Rpc>, read_only: Arc<AtomicBool>, pending_limit: Arc<AtomicUsize>) -> Self {
        Self {
            rpc,
            read_only,
            pending_limit,
        }
    }
}

/// Forward one ledger request and fold carrier errors back into [`LedgerError`].
async fn ledger_rpc(rpc: &Rpc, req: Request) -> Result<Response, LedgerError> {
    let resp = rpc
        .call(req)
        .await
        .map_err(|e| LedgerError::Store(StoreError::Internal(e.to_string())))?;
    match resp {
        Response::Err(ProtoError::Ledger(e)) => Err(e),
        Response::Err(ProtoError::Internal(s)) => Err(LedgerError::Store(StoreError::Internal(s))),
        ok => Ok(ok),
    }
}

#[async_trait]
impl ResponseLedger for MemLedgerClient {
    async fn create(
        &self,
        record: ResponseRecord,
        idempotency_key: IdempotencyKey,
        now_ms: u64,
    ) -> Result<CreateOutcome, LedgerError> {
        // Per-node admission (INV-32 / FR-33), mirroring the sql adapter's
        // local read_only + pending_limit against a shared in-flight count.
        if self.read_only.load(Ordering::SeqCst) {
            return Ok(CreateOutcome::ReadOnly);
        }
        let in_flight = self.in_flight().await?;
        if in_flight >= self.pending_limit.load(Ordering::SeqCst) {
            return Ok(CreateOutcome::Overloaded);
        }
        match ledger_rpc(
            &self.rpc,
            Request::LedgerCreate {
                record: Box::new(record),
                idempotency_key,
                now_ms,
            },
        )
        .await?
        {
            Response::Create(o) => Ok(o),
            other => Err(LedgerError::Store(StoreError::Internal(format!(
                "unexpected rpc response {other:?}"
            )))),
        }
    }

    async fn claim(
        &self,
        agent_id: AgentId,
        now_ms: u64,
        exec_ttl: Duration,
    ) -> Result<Option<ClaimedResponse>, LedgerError> {
        if self.read_only.load(Ordering::SeqCst) {
            return Err(LedgerError::Store(StoreError::ReadOnly));
        }
        match ledger_rpc(
            &self.rpc,
            Request::LedgerClaim {
                agent_id,
                now_ms,
                exec_ttl,
            },
        )
        .await?
        {
            Response::Claim(o) => Ok(o),
            other => Err(LedgerError::Store(StoreError::Internal(format!(
                "unexpected rpc response {other:?}"
            )))),
        }
    }

    async fn heartbeat(&self, agent_id: AgentId, now_ms: u64) -> Result<(), LedgerError> {
        match ledger_rpc(
            &self.rpc,
            Request::LedgerHeartbeat { agent_id, now_ms },
        )
        .await?
        {
            Response::Heartbeat => Ok(()),
            other => Err(LedgerError::Store(StoreError::Internal(format!(
                "unexpected rpc response {other:?}"
            )))),
        }
    }

    async fn complete(
        &self,
        response_id: &ResponseId,
        expected_attempt: Attempt,
        status: ResponseStatus,
        usage: Usage,
        now_ms: u64,
    ) -> Result<(), LedgerError> {
        if self.read_only.load(Ordering::SeqCst) {
            return Err(LedgerError::Store(StoreError::ReadOnly));
        }
        match ledger_rpc(
            &self.rpc,
            Request::LedgerComplete {
                response_id: response_id.clone(),
                expected_attempt,
                status,
                usage,
                now_ms,
            },
        )
        .await?
        {
            Response::Complete => Ok(()),
            other => Err(LedgerError::Store(StoreError::Internal(format!(
                "unexpected rpc response {other:?}"
            )))),
        }
    }

    async fn cancel(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
        now_ms: u64,
    ) -> Result<(), LedgerError> {
        if self.read_only.load(Ordering::SeqCst) {
            return Err(LedgerError::Store(StoreError::ReadOnly));
        }
        match ledger_rpc(
            &self.rpc,
            Request::LedgerCancel {
                tenant: tenant.clone(),
                response_id: response_id.clone(),
                now_ms,
            },
        )
        .await?
        {
            Response::Cancel => Ok(()),
            other => Err(LedgerError::Store(StoreError::Internal(format!(
                "unexpected rpc response {other:?}"
            )))),
        }
    }

    async fn reap(
        &self,
        now_ms: u64,
        heartbeat_ttl: Duration,
    ) -> Result<Vec<AbortedClaim>, LedgerError> {
        match ledger_rpc(
            &self.rpc,
            Request::LedgerReap {
                now_ms,
                heartbeat_ttl,
            },
        )
        .await?
        {
            Response::Reap(aborted) => Ok(aborted),
            other => Err(LedgerError::Store(StoreError::Internal(format!(
                "unexpected rpc response {other:?}"
            )))),
        }
    }

    async fn record_partial_usage(
        &self,
        response_id: &ResponseId,
        attempt: Attempt,
        usage: Usage,
    ) -> Result<(), LedgerError> {
        match ledger_rpc(
            &self.rpc,
            Request::LedgerRecordPartialUsage {
                response_id: response_id.clone(),
                attempt,
                usage,
            },
        )
        .await?
        {
            Response::RecordPartialUsage => Ok(()),
            other => Err(LedgerError::Store(StoreError::Internal(format!(
                "unexpected rpc response {other:?}"
            )))),
        }
    }

    async fn get(&self, response_id: &ResponseId) -> Result<Option<ResponseRecord>, LedgerError> {
        match ledger_rpc(
            &self.rpc,
            Request::LedgerGet {
                response_id: response_id.clone(),
            },
        )
        .await?
        {
            Response::Get(o) => Ok(o),
            other => Err(LedgerError::Store(StoreError::Internal(format!(
                "unexpected rpc response {other:?}"
            )))),
        }
    }

    async fn delete(&self, response_id: &ResponseId) -> Result<bool, LedgerError> {
        if self.read_only.load(Ordering::SeqCst) {
            return Err(LedgerError::Store(StoreError::ReadOnly));
        }
        match ledger_rpc(
            &self.rpc,
            Request::LedgerDelete {
                response_id: response_id.clone(),
            },
        )
        .await?
        {
            Response::Delete(removed) => Ok(removed),
            other => Err(LedgerError::Store(StoreError::Internal(format!(
                "unexpected rpc response {other:?}"
            )))),
        }
    }

    async fn delete_by_tenant(&self, tenant: &TenantId) -> Result<u64, LedgerError> {
        if self.read_only.load(Ordering::SeqCst) {
            return Err(LedgerError::Store(StoreError::ReadOnly));
        }
        match ledger_rpc(
            &self.rpc,
            Request::LedgerDeleteByTenant {
                tenant: tenant.clone(),
            },
        )
        .await?
        {
            Response::DeleteByTenant(n) => Ok(n),
            other => Err(LedgerError::Store(StoreError::Internal(format!(
                "unexpected rpc response {other:?}"
            )))),
        }
    }

    async fn check_attempt(
        &self,
        response_id: &ResponseId,
        attempt: Attempt,
    ) -> Result<(), LedgerError> {
        if self.read_only.load(Ordering::SeqCst) {
            return Err(LedgerError::Store(StoreError::ReadOnly));
        }
        match ledger_rpc(
            &self.rpc,
            Request::LedgerCheckAttempt {
                response_id: response_id.clone(),
                attempt,
            },
        )
        .await?
        {
            Response::CheckAttempt => Ok(()),
            other => Err(LedgerError::Store(StoreError::Internal(format!(
                "unexpected rpc response {other:?}"
            )))),
        }
    }

    async fn in_flight(&self) -> Result<usize, LedgerError> {
        match ledger_rpc(&self.rpc, Request::LedgerInFlight).await? {
            Response::InFlight(n) => Ok(n),
            other => Err(LedgerError::Store(StoreError::Internal(format!(
                "unexpected rpc response {other:?}"
            )))),
        }
    }

}

impl AdmissionControl for MemLedgerClient {
    fn set_read_only(&self, enabled: bool) {
        self.read_only.store(enabled, Ordering::SeqCst);
    }

    fn is_read_only(&self) -> bool {
        self.read_only.load(Ordering::SeqCst)
    }

    fn set_pending_limit(&self, limit: usize) {
        self.pending_limit.store(limit.max(1), Ordering::SeqCst);
    }

    fn pending_limit(&self) -> usize {
        self.pending_limit.load(Ordering::SeqCst)
    }
}
