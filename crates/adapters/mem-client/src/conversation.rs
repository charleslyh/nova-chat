//! Conversation store client: shared data forwarded to the carrier; per-node
//! read_only gate on writes (INV-32), unreachable → `Unavailable` (INV-46).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use nova_responses_core::{
    Conversation, ConversationError, ConversationId, ConversationStore, ResponseId, TenantId,
};

use adapters_mem::proto::{ProtoError, Request, Response};

use crate::rpc::Rpc;

pub struct MemConversationClient {
    rpc: Arc<Rpc>,
    read_only: Arc<AtomicBool>,
}

impl MemConversationClient {
    pub fn new(rpc: Arc<Rpc>, read_only: Arc<AtomicBool>) -> Self {
        Self { rpc, read_only }
    }

    fn guard_writable(&self) -> Result<(), ConversationError> {
        if self.read_only.load(Ordering::SeqCst) {
            return Err(ConversationError::ReadOnly);
        }
        Ok(())
    }
}

async fn conversation_rpc(rpc: &Rpc, req: Request) -> Result<Response, ConversationError> {
    // Transport failure = store unreachable. Callers must reject writes rather
    // than proceed unstored (INV-46).
    let resp = rpc
        .call(req)
        .await
        .map_err(|_| ConversationError::Unavailable)?;
    match resp {
        Response::Err(ProtoError::Conversation(e)) => Err(e),
        Response::Err(ProtoError::Internal(s)) => Err(ConversationError::Internal(s)),
        ok => Ok(ok),
    }
}

/// Reject a response shape the carrier should never have produced for this call.
fn unexpected(other: Response) -> ConversationError {
    ConversationError::Internal(format!("unexpected rpc response {other:?}"))
}

#[async_trait]
impl ConversationStore for MemConversationClient {
    async fn create(
        &self,
        conversation: Conversation,
    ) -> Result<Conversation, ConversationError> {
        self.guard_writable()?;
        match conversation_rpc(&self.rpc, Request::ConversationCreate { conversation }).await? {
            Response::ConversationCreate(c) => Ok(c),
            other => Err(unexpected(other)),
        }
    }

    async fn get(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<Option<Conversation>, ConversationError> {
        match conversation_rpc(
            &self.rpc,
            Request::ConversationGet {
                tenant: tenant.clone(),
                id: id.clone(),
            },
        )
        .await?
        {
            Response::ConversationGet(c) => Ok(c),
            other => Err(unexpected(other)),
        }
    }

    async fn update_metadata(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        metadata: BTreeMap<String, String>,
    ) -> Result<Conversation, ConversationError> {
        self.guard_writable()?;
        match conversation_rpc(
            &self.rpc,
            Request::ConversationUpdateMetadata {
                tenant: tenant.clone(),
                id: id.clone(),
                metadata,
            },
        )
        .await?
        {
            Response::ConversationUpdateMetadata(c) => Ok(c),
            other => Err(unexpected(other)),
        }
    }

    async fn delete(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<bool, ConversationError> {
        self.guard_writable()?;
        match conversation_rpc(
            &self.rpc,
            Request::ConversationDelete {
                tenant: tenant.clone(),
                id: id.clone(),
            },
        )
        .await?
        {
            Response::ConversationDelete(removed) => Ok(removed),
            other => Err(unexpected(other)),
        }
    }

    async fn delete_by_tenant(&self, tenant: &TenantId) -> Result<u64, ConversationError> {
        self.guard_writable()?;
        match conversation_rpc(
            &self.rpc,
            Request::ConversationDeleteByTenant {
                tenant: tenant.clone(),
            },
        )
        .await?
        {
            Response::ConversationDeleteByTenant(n) => Ok(n),
            other => Err(unexpected(other)),
        }
    }

    async fn advance(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        last: &ResponseId,
    ) -> Result<(), ConversationError> {
        self.guard_writable()?;
        match conversation_rpc(
            &self.rpc,
            Request::ConversationAdvance {
                tenant: tenant.clone(),
                id: id.clone(),
                last: last.clone(),
            },
        )
        .await?
        {
            Response::ConversationAdvance => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn health(&self) -> Result<(), ConversationError> {
        match conversation_rpc(&self.rpc, Request::ConversationHealth).await? {
            Response::ConversationHealth => Ok(()),
            other => Err(unexpected(other)),
        }
    }
}
