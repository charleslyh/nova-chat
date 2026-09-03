//! Carrier-side dispatch: maps a data-plane [`Request`] onto the in-process
//! port implementations and returns a [`Response`].
//!
//! Pure logic, no transport: the HTTP wiring lives in the `nova-responses-mem-server`
//! binary, so this stays unit-testable without a socket.

use nova_responses_core::{ContextStore, ResponseEventLog, ResponseLedger};

use crate::proto::{ProtoError, Request, Response};
use crate::MemWorld;

/// Execute one request against the shared world and produce the response.
///
/// `EventLogReadAfter` may block up to its `wait_ms` (long poll); everything
/// else returns promptly.
pub async fn dispatch(world: &MemWorld, req: Request) -> Response {
    match req {
        Request::LedgerCreate {
            record,
            idempotency_key,
            now_ms,
        } => match world.ledger.create(record, idempotency_key, now_ms).await {
            Ok(o) => Response::Create(o),
            Err(e) => Response::Err(ProtoError::Ledger(e)),
        },
        Request::LedgerClaim {
            agent_id,
            now_ms,
            exec_ttl_ms,
        } => match world.ledger.claim(agent_id, now_ms, exec_ttl_ms).await {
            Ok(o) => Response::Claim(o),
            Err(e) => Response::Err(ProtoError::Ledger(e)),
        },
        Request::LedgerHeartbeat { agent_id, now_ms } => {
            match world.ledger.heartbeat(agent_id, now_ms).await {
                Ok(()) => Response::Heartbeat,
                Err(e) => Response::Err(ProtoError::Ledger(e)),
            }
        }
        Request::LedgerComplete {
            response_id,
            expected_attempt,
            status,
            usage,
            now_ms,
        } => match world
            .ledger
            .complete(&response_id, expected_attempt, status, usage, now_ms)
            .await
        {
            Ok(()) => Response::Complete,
            Err(e) => Response::Err(ProtoError::Ledger(e)),
        },
        Request::LedgerCancel {
            tenant,
            response_id,
            now_ms,
        } => match world.ledger.cancel(&tenant, &response_id, now_ms).await {
            Ok(()) => Response::Cancel,
            Err(e) => Response::Err(ProtoError::Ledger(e)),
        },
        Request::LedgerReap {
            now_ms,
            heartbeat_ttl_ms,
        } => match world.ledger.reap(now_ms, heartbeat_ttl_ms).await {
            Ok(aborted) => Response::Reap(aborted),
            Err(e) => Response::Err(ProtoError::Ledger(e)),
        },
        Request::LedgerRecordPartialUsage {
            response_id,
            attempt,
            usage,
        } => match world
            .ledger
            .record_partial_usage(&response_id, attempt, usage)
            .await
        {
            Ok(()) => Response::RecordPartialUsage,
            Err(e) => Response::Err(ProtoError::Ledger(e)),
        },
        Request::LedgerGet { response_id } => match world.ledger.get(&response_id).await {
            Ok(o) => Response::Get(o),
            Err(e) => Response::Err(ProtoError::Ledger(e)),
        },
        Request::LedgerCheckAttempt {
            response_id,
            attempt,
        } => match world.ledger.check_attempt(&response_id, attempt).await {
            Ok(()) => Response::CheckAttempt,
            Err(e) => Response::Err(ProtoError::Ledger(e)),
        },
        Request::LedgerInFlight => match world.ledger.in_flight().await {
            Ok(n) => Response::InFlight(n),
            Err(e) => Response::Err(ProtoError::Ledger(e)),
        },

        Request::EventLogAppend { event } => {
            match world.event_log.append(event.into()).await {
                Ok(seq) => Response::EventLogAppend(seq),
                Err(e) => Response::Err(ProtoError::EventLog(e)),
            }
        }
        Request::EventLogReadAfter {
            response_id,
            starting_after,
            limit,
            wait_ms,
        } => match world
            .event_log
            .read_after(&response_id, starting_after, limit, wait_ms)
            .await
        {
            Ok(events) => Response::EventLogReadAfter(events.into_iter().map(Into::into).collect()),
            Err(e) => Response::Err(ProtoError::EventLog(e)),
        },
        Request::EventLogClose {
            response_id,
            now_ms,
            retain_ms,
        } => match world.event_log.close(&response_id, now_ms, retain_ms).await {
            Ok(()) => Response::EventLogClose,
            Err(e) => Response::Err(ProtoError::EventLog(e)),
        },
        Request::EventLogSweepExpired { now_ms } => {
            match world.event_log.sweep_expired(now_ms).await {
                Ok(n) => Response::EventLogSweepExpired(n),
                Err(e) => Response::Err(ProtoError::EventLog(e)),
            }
        }

        Request::ContextPut { record } => match world.context.put(record).await {
            Ok(()) => Response::ContextPut,
            Err(e) => Response::Err(ProtoError::Context(e)),
        },
        Request::ContextAppendOutput {
            tenant,
            response_id,
            items,
            usage,
            status,
            now_ms,
        } => match world
            .context
            .append_output(&tenant, &response_id, items, usage, status, now_ms)
            .await
        {
            Ok(()) => Response::ContextAppendOutput,
            Err(e) => Response::Err(ProtoError::Context(e)),
        },
        Request::ContextGet {
            tenant,
            response_id,
        } => match world.context.get(&tenant, &response_id).await {
            Ok(o) => Response::ContextGet(o),
            Err(e) => Response::Err(ProtoError::Context(e)),
        },
        Request::ContextResolveChain {
            tenant,
            from,
            limits,
        } => match world.context.resolve_chain(&tenant, &from, limits).await {
            Ok(ctx) => Response::ContextResolveChain(ctx),
            Err(e) => Response::Err(ProtoError::Context(e)),
        },
        Request::ContextDelete {
            tenant,
            response_id,
        } => match world.context.delete(&tenant, &response_id).await {
            Ok(removed) => Response::ContextDelete(removed),
            Err(e) => Response::Err(ProtoError::Context(e)),
        },
        Request::ContextDeleteByTenant { tenant } => {
            match world.context.delete_by_tenant(&tenant).await {
                Ok(n) => Response::ContextDeleteByTenant(n),
                Err(e) => Response::Err(ProtoError::Context(e)),
            }
        }
        Request::ContextSweepExpired { now_ms, limit } => {
            match world.context.sweep_expired(now_ms, limit).await {
                Ok(n) => Response::ContextSweepExpired(n),
                Err(e) => Response::Err(ProtoError::Context(e)),
            }
        }
        Request::ContextHealth => match world.context.health().await {
            Ok(()) => Response::ContextHealth,
            Err(e) => Response::Err(ProtoError::Context(e)),
        },
    }
}
