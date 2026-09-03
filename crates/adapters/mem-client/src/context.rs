//! Context store client: shared data forwarded to the carrier; per-node
//! read_only gate on writes (INV-32), unreachable → `Unavailable` (INV-46).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use nova_responses_core::{
    ChainLimits, ContextError, ContextStore, ResolvedContext, ResponseId, ResponseItem,
    ResponseStatus, StoredResponse, TenantId, Usage,
};

use adapters_mem::proto::{ProtoError, Request, Response};

use crate::rpc::Rpc;

pub struct MemContextClient {
    rpc: Arc<Rpc>,
    read_only: Arc<AtomicBool>,
}

impl MemContextClient {
    pub fn new(rpc: Arc<Rpc>, read_only: Arc<AtomicBool>) -> Self {
        Self { rpc, read_only }
    }
}

async fn context_rpc(rpc: &Rpc, req: Request) -> Result<Response, ContextError> {
    // Transport failure = store unreachable. Callers must reject writes rather
    // than proceed unstored (INV-46).
    let resp = rpc.call(req).await.map_err(|_| ContextError::Unavailable)?;
    match resp {
        Response::Err(ProtoError::Context(e)) => Err(e),
        Response::Err(ProtoError::Internal(s)) => Err(ContextError::Internal(s)),
        ok => Ok(ok),
    }
}

#[async_trait]
impl ContextStore for MemContextClient {
    async fn put(&self, record: StoredResponse) -> Result<(), ContextError> {
        if self.read_only.load(Ordering::SeqCst) {
            return Err(ContextError::ReadOnly);
        }
        match context_rpc(&self.rpc, Request::ContextPut { record }).await? {
            Response::ContextPut => Ok(()),
            other => Err(ContextError::Internal(format!(
                "unexpected rpc response {other:?}"
            ))),
        }
    }

    async fn append_output(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
        items: Vec<ResponseItem>,
        usage: Usage,
        status: ResponseStatus,
        now_ms: u64,
    ) -> Result<(), ContextError> {
        if self.read_only.load(Ordering::SeqCst) {
            return Err(ContextError::ReadOnly);
        }
        match context_rpc(
            &self.rpc,
            Request::ContextAppendOutput {
                tenant: tenant.clone(),
                response_id: response_id.clone(),
                items,
                usage,
                status,
                now_ms,
            },
        )
        .await?
        {
            Response::ContextAppendOutput => Ok(()),
            other => Err(ContextError::Internal(format!(
                "unexpected rpc response {other:?}"
            ))),
        }
    }

    async fn get(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
    ) -> Result<Option<StoredResponse>, ContextError> {
        match context_rpc(
            &self.rpc,
            Request::ContextGet {
                tenant: tenant.clone(),
                response_id: response_id.clone(),
            },
        )
        .await?
        {
            Response::ContextGet(o) => Ok(o),
            other => Err(ContextError::Internal(format!(
                "unexpected rpc response {other:?}"
            ))),
        }
    }

    async fn resolve_chain(
        &self,
        tenant: &TenantId,
        from: &ResponseId,
        limits: ChainLimits,
    ) -> Result<ResolvedContext, ContextError> {
        match context_rpc(
            &self.rpc,
            Request::ContextResolveChain {
                tenant: tenant.clone(),
                from: from.clone(),
                limits,
            },
        )
        .await?
        {
            Response::ContextResolveChain(ctx) => Ok(ctx),
            other => Err(ContextError::Internal(format!(
                "unexpected rpc response {other:?}"
            ))),
        }
    }

    async fn delete(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
    ) -> Result<bool, ContextError> {
        if self.read_only.load(Ordering::SeqCst) {
            return Err(ContextError::ReadOnly);
        }
        match context_rpc(
            &self.rpc,
            Request::ContextDelete {
                tenant: tenant.clone(),
                response_id: response_id.clone(),
            },
        )
        .await?
        {
            Response::ContextDelete(removed) => Ok(removed),
            other => Err(ContextError::Internal(format!(
                "unexpected rpc response {other:?}"
            ))),
        }
    }

    async fn delete_by_tenant(&self, tenant: &TenantId) -> Result<u64, ContextError> {
        if self.read_only.load(Ordering::SeqCst) {
            return Err(ContextError::ReadOnly);
        }
        match context_rpc(
            &self.rpc,
            Request::ContextDeleteByTenant {
                tenant: tenant.clone(),
            },
        )
        .await?
        {
            Response::ContextDeleteByTenant(n) => Ok(n),
            other => Err(ContextError::Internal(format!(
                "unexpected rpc response {other:?}"
            ))),
        }
    }

    async fn sweep_expired(&self, now_ms: u64, limit: usize) -> Result<u64, ContextError> {
        match context_rpc(
            &self.rpc,
            Request::ContextSweepExpired { now_ms, limit },
        )
        .await?
        {
            Response::ContextSweepExpired(n) => Ok(n),
            other => Err(ContextError::Internal(format!(
                "unexpected rpc response {other:?}"
            ))),
        }
    }

    async fn health(&self) -> Result<(), ContextError> {
        match context_rpc(&self.rpc, Request::ContextHealth).await? {
            Response::ContextHealth => Ok(()),
            other => Err(ContextError::Internal(format!(
                "unexpected rpc response {other:?}"
            ))),
        }
    }
}
