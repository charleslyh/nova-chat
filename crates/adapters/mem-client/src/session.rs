//! Session store client: shared data forwarded to the carrier; per-node
//! read_only gate on writes (INV-32), unreachable → `Unavailable` (INV-46).
//!
//! `begin_turn` and `end_turn` are single round trips, not a lock write followed
//! by an append. The port promises those two effects are atomic, and splitting
//! them across the wire would put a dropped connection between the halves.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use nova_responses_core::{
    ConversationId, ResponseId, ResponseStatus, Session, SessionError, SessionEvent,
    SessionEventKind, SessionId, SessionStore, TenantId,
};

use adapters_mem::proto::{ProtoError, Request, Response};

use crate::rpc::Rpc;

pub struct MemSessionClient {
    rpc: Arc<Rpc>,
    read_only: Arc<AtomicBool>,
}

impl MemSessionClient {
    pub fn new(rpc: Arc<Rpc>, read_only: Arc<AtomicBool>) -> Self {
        Self { rpc, read_only }
    }

    fn guard_writable(&self) -> Result<(), SessionError> {
        if self.read_only.load(Ordering::SeqCst) {
            return Err(SessionError::ReadOnly);
        }
        Ok(())
    }

    /// The three appending operations share one response variant, so they share
    /// one unwrap.
    async fn seq(&self, req: Request) -> Result<u64, SessionError> {
        match session_rpc(&self.rpc, req).await? {
            Response::SessionSeq(seq) => Ok(seq),
            other => Err(unexpected(other)),
        }
    }
}

async fn session_rpc(rpc: &Rpc, req: Request) -> Result<Response, SessionError> {
    // Transport failure = store unreachable. Callers must reject writes rather
    // than proceed unstored (INV-46).
    let resp = rpc.call(req).await.map_err(|_| SessionError::Unavailable)?;
    match resp {
        Response::Err(ProtoError::Session(e)) => Err(e),
        Response::Err(ProtoError::Internal(s)) => Err(SessionError::Internal(s)),
        ok => Ok(ok),
    }
}

fn unexpected(other: Response) -> SessionError {
    SessionError::Internal(format!("unexpected rpc response {other:?}"))
}

#[async_trait]
impl SessionStore for MemSessionClient {
    async fn create(&self, session: Session, now_ms: u64) -> Result<Session, SessionError> {
        self.guard_writable()?;
        match session_rpc(&self.rpc, Request::SessionCreate { session, now_ms }).await? {
            Response::SessionCreate(s) => Ok(s),
            other => Err(unexpected(other)),
        }
    }

    async fn get(
        &self,
        tenant: &TenantId,
        id: &SessionId,
    ) -> Result<Option<Session>, SessionError> {
        match session_rpc(
            &self.rpc,
            Request::SessionGet {
                tenant: tenant.clone(),
                id: id.clone(),
            },
        )
        .await?
        {
            Response::SessionGet(s) => Ok(s),
            other => Err(unexpected(other)),
        }
    }

    async fn get_by_conversation(
        &self,
        tenant: &TenantId,
        conversation: &ConversationId,
    ) -> Result<Option<Session>, SessionError> {
        match session_rpc(
            &self.rpc,
            Request::SessionGetByConversation {
                tenant: tenant.clone(),
                conversation: conversation.clone(),
            },
        )
        .await?
        {
            Response::SessionGet(s) => Ok(s),
            other => Err(unexpected(other)),
        }
    }

    async fn list(&self, tenant: &TenantId) -> Result<Vec<Session>, SessionError> {
        match session_rpc(
            &self.rpc,
            Request::SessionList {
                tenant: tenant.clone(),
            },
        )
        .await?
        {
            Response::SessionList(s) => Ok(s),
            other => Err(unexpected(other)),
        }
    }

    async fn delete(&self, tenant: &TenantId, id: &SessionId) -> Result<bool, SessionError> {
        self.guard_writable()?;
        match session_rpc(
            &self.rpc,
            Request::SessionDelete {
                tenant: tenant.clone(),
                id: id.clone(),
            },
        )
        .await?
        {
            Response::SessionDelete(removed) => Ok(removed),
            other => Err(unexpected(other)),
        }
    }

    async fn delete_by_tenant(&self, tenant: &TenantId) -> Result<u64, SessionError> {
        self.guard_writable()?;
        match session_rpc(
            &self.rpc,
            Request::SessionDeleteByTenant {
                tenant: tenant.clone(),
            },
        )
        .await?
        {
            Response::SessionDeleteByTenant(n) => Ok(n),
            other => Err(unexpected(other)),
        }
    }

    async fn begin_turn(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        response_id: &ResponseId,
        now_ms: u64,
    ) -> Result<u64, SessionError> {
        self.guard_writable()?;
        self.seq(Request::SessionBeginTurn {
            tenant: tenant.clone(),
            id: id.clone(),
            response_id: response_id.clone(),
            now_ms,
        })
        .await
    }

    async fn end_turn(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        response_id: &ResponseId,
        status: ResponseStatus,
        now_ms: u64,
    ) -> Result<u64, SessionError> {
        self.guard_writable()?;
        self.seq(Request::SessionEndTurn {
            tenant: tenant.clone(),
            id: id.clone(),
            response_id: response_id.clone(),
            status,
            now_ms,
        })
        .await
    }

    async fn release_stale_lock(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        holder: &ResponseId,
    ) -> Result<bool, SessionError> {
        self.guard_writable()?;
        match session_rpc(
            &self.rpc,
            Request::SessionReleaseStaleLock {
                tenant: tenant.clone(),
                id: id.clone(),
                holder: holder.clone(),
            },
        )
        .await?
        {
            Response::SessionReleaseStaleLock(released) => Ok(released),
            other => Err(unexpected(other)),
        }
    }

    async fn append_event(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        kind: SessionEventKind,
        now_ms: u64,
    ) -> Result<u64, SessionError> {
        self.guard_writable()?;
        self.seq(Request::SessionAppendEvent {
            tenant: tenant.clone(),
            id: id.clone(),
            kind,
            now_ms,
        })
        .await
    }

    async fn read_after(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        starting_after: Option<u64>,
        limit: usize,
        wait_ms: u64,
    ) -> Result<Vec<SessionEvent>, SessionError> {
        match session_rpc(
            &self.rpc,
            Request::SessionReadAfter {
                tenant: tenant.clone(),
                id: id.clone(),
                starting_after,
                limit,
                wait_ms,
            },
        )
        .await?
        {
            Response::SessionReadAfter(events) => Ok(events),
            other => Err(unexpected(other)),
        }
    }

    async fn health(&self) -> Result<(), SessionError> {
        match session_rpc(&self.rpc, Request::SessionHealth).await? {
            Response::SessionHealth => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    fn set_max_events_per_session(&self, limit: usize) {
        // Best effort: the carrier applies the bound; a dropped connection here
        // leaves the old bound in force, which is safe (a higher bound can only
        // admit more, never corrupt).
        let rpc = self.rpc.clone();
        tokio::spawn(async move {
            let _ = rpc
                .call(Request::SessionSetMaxEvents { limit })
                .await;
        });
    }
}
