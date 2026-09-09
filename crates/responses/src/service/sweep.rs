//! The sweeper, owned by the responses service.
//!
//! Reaping lost claims and releasing expired event buffers run on the same tick. Three
//! separate loops would mean three timers and three chances to forget one.
//!
//! The loop lives here, in the capability layer, because reaping a lost claim is a
//! response-lifecycle transition — raise the attempt fence, emit `Failed`, close the
//! stream, release the turn marker — so it belongs beside the other lifecycle
//! transitions (`create`, `cancel`), not in the assembly layer or a standalone process.

use std::time::Duration;

use tokio::sync::watch;
use tracing::warn;

use crate::clock::Clock;
use crate::events::{AppendEvent, ResponseEventKind};
use crate::ports::{metric, MetricsSink, ResponseEventLog, ResponseLedger, TurnLock};
use crate::protocol::ResponseObject;
use crate::response::ResponseStatus;

use super::responses::ResponsesDeps;

const TICK: Duration = Duration::from_secs(2);

/// Spawn the background loop. Every peer runs one; reaping is idempotent, so the
/// redundant loops race harmlessly.
///
/// The loop exits when `shutdown` is closed (its sender dropped by
/// [`crate::service::ResponsesService::stop`]) or a value is sent on it — at the next
/// tick boundary, without interrupting an in-flight tick.
pub(crate) fn spawn(shutdown: watch::Receiver<()>, deps: &ResponsesDeps) {
    let ledger = deps.ledger.clone();
    let event_log = deps.event_log.clone();
    let turn_lock = deps.turn_lock.clone();
    let clock = deps.clock.clone();
    let metrics = deps.metrics.clone();
    let heartbeat_ttl = deps.cfg.heartbeat_ttl();
    let retain_after_terminal = deps.cfg.retain_after_terminal();

    tokio::spawn(async move {
        let mut shutdown = shutdown;
        loop {
            tokio::select! {
                // `changed()` resolves with an error once the sender is dropped, so a
                // `stop` that drops the sender is as much a signal as an explicit send.
                _ = shutdown.changed() => break,
                _ = tokio::time::sleep(TICK) => {
                    tick(
                        ledger.as_ref(),
                        event_log.as_ref(),
                        turn_lock.as_ref(),
                        clock.as_ref(),
                        metrics.as_ref(),
                        heartbeat_ttl,
                        retain_after_terminal,
                    )
                    .await;
                }
            }
        }
    });
}

async fn tick(
    ledger: &dyn ResponseLedger,
    event_log: &dyn ResponseEventLog,
    turn_lock: &dyn TurnLock,
    clock: &dyn Clock,
    metrics: &dyn MetricsSink,
    heartbeat_ttl: Duration,
    retain_after_terminal: Duration,
) {
    let now = clock.now_ms();

    // 1. Reap claims whose holder stopped heartbeating. The ledger raises the attempt
    //    fence, so the dead holder cannot append afterwards (INV-6).
    let aborted = match ledger.reap(now, heartbeat_ttl).await {
        Ok(aborted) => aborted,
        Err(e) => {
            warn!(error = %e, "sweeper reap failed");
            return;
        }
    };

    for claim in aborted {
        // Partial usage is booked by the ledger itself during reaping, so a crash
        // between the two cannot lose it (INV-51).
        //
        // The terminal event carries a stub object: the full record could be read back
        // now that the claim carries a tenant, but that would be an extra read per
        // reaped claim to enrich an event whose only job is to end the stream.
        // Subscribers that want the finished object use `GET`.
        let _ = event_log
            .append(AppendEvent::lifecycle(
                claim.response_id.clone(),
                ResponseEventKind::Failed,
                ResponseObject::terminal_stub(&claim.response_id, ResponseStatus::Failed),
            ))
            .await;
        let _ = event_log
            .close(&claim.response_id, now, retain_after_terminal)
            .await;

        // Release the in-flight marker. `Failed` rather than a status of its own: from a
        // client's point of view a reaped turn is a failed turn, and inventing a sixth
        // status would oblige every subscriber to learn one.
        if let Some(conversation_id) = &claim.conversation_id {
            if let Err(e) = turn_lock
                .release_active(
                    &claim.tenant_id,
                    conversation_id,
                    &claim.response_id,
                    ResponseStatus::Failed,
                    now,
                )
                .await
            {
                // Not retried here: the reap already moved the record out of
                // `in_progress`, so the next tick will not select it again. The recovery
                // is on the admission side instead — a marker whose holder is already
                // terminal is taken over by the next `acquire_active`.
                warn!(
                    response = %claim.response_id,
                    conversation = %conversation_id,
                    error = %e,
                    "could not release the turn marker for a reaped claim; the next \
                     turn on this conversation will take the stale marker over"
                );
            }
        }

        metrics.incr(metric::RESPONSES_REAPED, 1);
    }

    // 2. Release event buffers past their retention window.
    match event_log.sweep_expired(now).await {
        Ok(0) => {}
        Ok(n) => metrics.incr(metric::EVENT_LOGS_SWEPT, n),
        Err(e) => warn!(error = %e, "event log sweep failed"),
    }
}
