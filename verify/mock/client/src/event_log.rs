//! Event log client: appends/reads forwarded to the carrier; per-node read_only
//! gate on append (INV-32).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nova_responses::{AppendEvent, ResponseEvent, ResponseId};
use nova_responses::ports::{EventLogError, ResponseEventLog, StoreError};

use mock_server::proto::{ProtoError, Request, Response};

use crate::rpc::Rpc;

pub struct MemEventLogClient {
    rpc: Arc<Rpc>,
    read_only: Arc<AtomicBool>,
}

impl MemEventLogClient {
    pub fn new(rpc: Arc<Rpc>, read_only: Arc<AtomicBool>) -> Self {
        Self { rpc, read_only }
    }
}

async fn event_rpc(rpc: &Rpc, req: Request) -> Result<Response, EventLogError> {
    let resp = rpc
        .call(req)
        .await
        .map_err(|e| EventLogError::Store(StoreError::Internal(e.to_string())))?;
    match resp {
        Response::Err(ProtoError::EventLog(e)) => Err(e),
        Response::Err(ProtoError::Internal(s)) => Err(EventLogError::Store(StoreError::Internal(s))),
        ok => Ok(ok),
    }
}

#[async_trait]
impl ResponseEventLog for MemEventLogClient {
    async fn append(&self, event: AppendEvent) -> Result<u64, EventLogError> {
        if self.read_only.load(Ordering::SeqCst) {
            return Err(EventLogError::Store(StoreError::ReadOnly));
        }
        match event_rpc(&self.rpc, Request::EventLogAppend { event: event.into() }).await? {
            Response::EventLogAppend(seq) => Ok(seq),
            other => Err(EventLogError::Store(StoreError::Internal(format!(
                "unexpected rpc response {other:?}"
            )))),
        }
    }

    async fn read_after(
        &self,
        response_id: &ResponseId,
        starting_after: Option<u64>,
        limit: usize,
        wait: Duration,
    ) -> Result<Vec<ResponseEvent>, EventLogError> {
        match event_rpc(
            &self.rpc,
            Request::EventLogReadAfter {
                response_id: response_id.clone(),
                starting_after,
                limit,
                wait,
            },
        )
        .await?
        {
            // A read that came back without its sequence number is a carrier defect, not
            // a number to invent: every cursor downstream depends on it.
            Response::EventLogReadAfter(events) => events
                .into_iter()
                .map(|e| {
                    e.into_response_event()
                        .map_err(|e| EventLogError::Store(StoreError::Internal(e.to_string())))
                })
                .collect(),
            other => Err(EventLogError::Store(StoreError::Internal(format!(
                "unexpected rpc response {other:?}"
            )))),
        }
    }

    async fn close(
        &self,
        response_id: &ResponseId,
        now_ms: u64,
        retain: Duration,
    ) -> Result<(), EventLogError> {
        match event_rpc(
            &self.rpc,
            Request::EventLogClose {
                response_id: response_id.clone(),
                now_ms,
                retain,
            },
        )
        .await?
        {
            Response::EventLogClose => Ok(()),
            other => Err(EventLogError::Store(StoreError::Internal(format!(
                "unexpected rpc response {other:?}"
            )))),
        }
    }

    async fn sweep_expired(&self, now_ms: u64) -> Result<u64, EventLogError> {
        match event_rpc(&self.rpc, Request::EventLogSweepExpired { now_ms }).await? {
            Response::EventLogSweepExpired(n) => Ok(n),
            other => Err(EventLogError::Store(StoreError::Internal(format!(
                "unexpected rpc response {other:?}"
            )))),
        }
    }

    async fn remove(&self, response_id: &ResponseId) -> Result<(), EventLogError> {
        match event_rpc(
            &self.rpc,
            Request::EventLogRemove {
                response_id: response_id.clone(),
            },
        )
        .await?
        {
            Response::EventLogRemove => Ok(()),
            other => Err(EventLogError::Store(StoreError::Internal(format!(
                "unexpected rpc response {other:?}"
            )))),
        }
    }
}
