//! Background maintenance, merged into a single loop.
//!
//! Reaping lost claims, releasing expired event buffers and clearing expired
//! content all run on the same tick. Three separate loops would mean three
//! timers and three chances to forget one.
//!
//! Takes the ports directly (rather than the whole [`crate::AppState`]) so the
//! same loop can run embedded in the gateway **or** in a standalone sweep
//! process that mounts the shared carrier through the client adapters.

use std::sync::Arc;
use std::time::Duration;

use nova_responses_core::{
    AppendEvent, ContextStore, ConversationStore, MetricsSink, ResponseEventKind,
    ResponseEventLog, ResponseLedger, ResponseStatus,
};
use tracing::warn;

const TICK: Duration = Duration::from_secs(2);
/// Bounded per pass so one tick cannot turn into a long transaction.
const SWEEP_BATCH: usize = 500;

/// The ports and knobs the sweep loop needs, independent of any HTTP surface.
pub struct SweepDeps {
    pub ledger: Arc<dyn ResponseLedger>,
    pub event_log: Arc<dyn ResponseEventLog>,
    pub context: Arc<dyn ContextStore>,
    /// Reaping is a terminal transition, so it owes the conversation a marker
    /// release (D28). It is also the **only** release a reaped response gets: its
    /// holder is gone and the fence has moved, so that holder's own terminal path
    /// is refused as stale. Without this the conversation stays busy forever. A
    /// reaped turn committed no output, so there is no tail to advance.
    pub conversations: Arc<dyn ConversationStore>,
    pub now: Arc<dyn Fn() -> u64 + Send + Sync>,
    pub metrics: Arc<dyn MetricsSink>,
    pub heartbeat_ttl_ms: u64,
    pub retain_after_terminal_ms: u64,
}

pub fn spawn(deps: SweepDeps) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(TICK).await;
            if let Err(e) = tick(&deps).await {
                // Logged without payloads: only ids and codes ever reach the log
                // (SEC-9).
                warn!(error = %e, "sweeper tick failed");
            }
        }
    });
}

async fn tick(deps: &SweepDeps) -> anyhow::Result<()> {
    let now = (deps.now)();

    // 1. Reap claims whose holder stopped heartbeating. The ledger raises the
    //    attempt fence, so the dead holder cannot append afterwards (INV-6).
    let aborted = deps
        .ledger
        .reap(now, deps.heartbeat_ttl_ms)
        .await
        .map_err(|e| anyhow::anyhow!("reap: {e}"))?;

    for claim in aborted {
        // Partial usage is booked by the ledger itself during reaping, so a
        // crash between the two cannot lose it (INV-51).
        //
        // The terminal event carries a minimal response object: the full record
        // could be read back now that the claim carries a tenant, but that would
        // be an extra read per reaped claim to enrich an event whose only job is
        // to end the stream. Subscribers that want the finished object use `GET`.
        let response = serde_json::json!({
            "id": claim.response_id.to_string(),
            "object": "response",
            "status": "failed",
        });
        let _ = deps
            .event_log
            .append(AppendEvent::lifecycle(
                claim.response_id.clone(),
                ResponseEventKind::Failed,
                response,
            ))
            .await;
        let _ = deps
            .event_log
            .close(&claim.response_id, now, deps.retain_after_terminal_ms)
            .await;

        // Release the in-flight marker. `Failed` rather than a status of its own:
        // from a client's point of view a reaped turn is a failed turn, and
        // inventing a sixth status would oblige every subscriber to learn one.
        if let Some(conversation_id) = &claim.conversation_id {
            if let Err(e) = deps
                .conversations
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
                // `in_progress`, so the next tick will not select it again. The
                // recovery is on the admission side instead — a marker whose holder
                // is already terminal is taken over by the next `acquire_active`
                // (see `ResponsesService::acquire_turn`).
                warn!(
                    response = %claim.response_id,
                    conversation = %conversation_id,
                    error = %e,
                    "could not release the turn marker for a reaped claim; the next \
                     turn on this conversation will take the stale marker over"
                );
            }
        }

        deps.metrics.incr("responses_reaped", 1).await;
    }

    // 2. Release event buffers past their retention window.
    match deps.event_log.sweep_expired(now).await {
        Ok(0) => {}
        Ok(n) => deps.metrics.incr("event_logs_swept", n).await,
        Err(e) => warn!(error = %e, "event log sweep failed"),
    }

    // 3. Clear expired stored content (FR-22 / OR-5).
    match deps.context.sweep_expired(now, SWEEP_BATCH).await {
        Ok(0) => {}
        Ok(n) => deps.metrics.incr("content_expired", n).await,
        Err(e) => warn!(error = %e, "content sweep failed"),
    }

    Ok(())
}
