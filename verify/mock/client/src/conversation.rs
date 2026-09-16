//! Conversation store client: shared data forwarded to the carrier; per-node read_only
//! gate on writes (INV-32), unreachable → `Unavailable` (INV-46).
//!
//! Four impl blocks, one per port facet (records, snapshot, turn lock, event stream). The
//! composition [`nova_responses::ports::ConversationStore`] comes free from its blanket
//! impl, so assembly still mounts one object.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nova_responses::ports::{
    ConversationError, ConversationEvents, ConversationRepo, ConversationSnapshots, StoreError,
    TurnLock,
};
use nova_responses::protocol::MetadataValue;
use nova_responses::{
    Conversation, ConversationEvent, ConversationEventKind, ConversationId, ResolvedContext,
    ResponseId, ResponseStatus, TenantId, TurnCommit,
};

use mock_server::proto::{ProtoError, Request, Response};

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
            return Err(ConversationError::Store(StoreError::ReadOnly));
        }
        Ok(())
    }
}

async fn conversation_rpc(rpc: &Rpc, req: Request) -> Result<Response, ConversationError> {
    // Transport failure = store unreachable. Callers must reject writes rather than
    // proceed unstored (INV-46).
    let resp = rpc
        .call(req)
        .await
        .map_err(|_| ConversationError::Store(StoreError::Unavailable))?;
    match resp {
        Response::Err(ProtoError::Conversation(e)) => Err(e),
        Response::Err(ProtoError::Internal(s)) => {
            Err(ConversationError::Store(StoreError::Internal(s)))
        }
        ok => Ok(ok),
    }
}

/// Reject a response shape the carrier should never have produced for this call.
fn unexpected(other: Response) -> ConversationError {
    ConversationError::Store(StoreError::Internal(format!(
        "unexpected rpc response {other:?}"
    )))
}

#[async_trait]
impl ConversationRepo for MemConversationClient {
    async fn create(&self, conversation: Conversation) -> Result<Conversation, ConversationError> {
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
        metadata: BTreeMap<String, MetadataValue>,
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

    async fn list(&self, tenant: &TenantId) -> Result<Vec<Conversation>, ConversationError> {
        match conversation_rpc(
            &self.rpc,
            Request::ConversationList {
                tenant: tenant.clone(),
            },
        )
        .await?
        {
            Response::ConversationList(list) => Ok(list),
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

#[async_trait]
impl ConversationSnapshots for MemConversationClient {
    async fn read_snapshot(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<ResolvedContext, ConversationError> {
        match conversation_rpc(
            &self.rpc,
            Request::ConversationReadSnapshot {
                tenant: tenant.clone(),
                id: id.clone(),
            },
        )
        .await?
        {
            Response::ConversationReadSnapshot(ctx) => Ok(ctx),
            other => Err(unexpected(other)),
        }
    }

    async fn append_turn(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        response_id: &ResponseId,
        commit: TurnCommit,
        now_ms: u64,
    ) -> Result<u64, ConversationError> {
        self.guard_writable()?;
        // The commit travels whole: taking it apart here and reassembling it on the
        // carrier was five fields' worth of opportunity to mismatch.
        match conversation_rpc(
            &self.rpc,
            Request::ConversationAppendTurn {
                tenant: tenant.clone(),
                id: id.clone(),
                response_id: response_id.clone(),
                commit,
                now_ms,
            },
        )
        .await?
        {
            Response::ConversationAppendTurn(idx) => Ok(idx),
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
}

#[async_trait]
impl TurnLock for MemConversationClient {
    async fn acquire_active(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        response_id: &ResponseId,
        now_ms: u64,
    ) -> Result<u64, ConversationError> {
        self.guard_writable()?;
        match conversation_rpc(
            &self.rpc,
            Request::ConversationAcquireActive {
                tenant: tenant.clone(),
                id: id.clone(),
                response_id: response_id.clone(),
                now_ms,
            },
        )
        .await?
        {
            Response::ConversationAcquireActive(seq) => Ok(seq),
            other => Err(unexpected(other)),
        }
    }

    async fn release_active(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        response_id: &ResponseId,
        status: ResponseStatus,
        now_ms: u64,
    ) -> Result<u64, ConversationError> {
        self.guard_writable()?;
        match conversation_rpc(
            &self.rpc,
            Request::ConversationReleaseActive {
                tenant: tenant.clone(),
                id: id.clone(),
                response_id: response_id.clone(),
                status,
                now_ms,
            },
        )
        .await?
        {
            Response::ConversationReleaseActive(seq) => Ok(seq),
            other => Err(unexpected(other)),
        }
    }

    async fn release_stale_active(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        holder: &ResponseId,
    ) -> Result<bool, ConversationError> {
        self.guard_writable()?;
        match conversation_rpc(
            &self.rpc,
            Request::ConversationReleaseStaleActive {
                tenant: tenant.clone(),
                id: id.clone(),
                holder: holder.clone(),
            },
        )
        .await?
        {
            Response::ConversationReleaseStaleActive(released) => Ok(released),
            other => Err(unexpected(other)),
        }
    }
}

#[async_trait]
impl ConversationEvents for MemConversationClient {
    async fn append_event(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        kind: ConversationEventKind,
        now_ms: u64,
    ) -> Result<u64, ConversationError> {
        self.guard_writable()?;
        match conversation_rpc(
            &self.rpc,
            Request::ConversationAppendEvent {
                tenant: tenant.clone(),
                id: id.clone(),
                kind,
                now_ms,
            },
        )
        .await?
        {
            Response::ConversationAppendEvent(seq) => Ok(seq),
            other => Err(unexpected(other)),
        }
    }

    async fn read_after(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        starting_after: Option<u64>,
        limit: usize,
        wait: Duration,
    ) -> Result<Vec<ConversationEvent>, ConversationError> {
        match conversation_rpc(
            &self.rpc,
            Request::ConversationReadAfter {
                tenant: tenant.clone(),
                id: id.clone(),
                starting_after,
                limit,
                wait,
            },
        )
        .await?
        {
            Response::ConversationReadAfter(events) => Ok(events),
            other => Err(unexpected(other)),
        }
    }

    fn set_max_events(&self, _limit: usize) {
        // The bound lives on the carrier (mem-server); the client has no local knob to
        // flip. No-op here, mirroring the other client-side runtime controls.
    }
}
