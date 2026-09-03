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
    Clock, ContextStore, MetricsSink, ResponseEvent, ResponseEventKind, ResponseEventLog,
    ResponseLedger,
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
    pub clock: Arc<dyn Clock>,
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
    let now = deps.clock.now_ms().await;

    // 1. Reap claims whose holder stopped heartbeating. The ledger raises the
    //    attempt fence, so the dead holder cannot append afterwards (INV-6).
    let aborted = deps
        .ledger
        .reap(now, deps.heartbeat_ttl_ms)
        .await
        .map_err(|e| anyhow::anyhow!("reap: {e}"))?;

    for claim in aborted {
        // Partial usage is booked by the ledger itself during reaping, so a
        // crash between the two cannot lose it (INV-51). The reap path has no
        // tenant handle, so the terminal event carries a minimal response object.
        let response = serde_json::json!({
            "id": claim.response_id.to_string(),
            "object": "response",
            "status": "failed",
        });
        let _ = deps
            .event_log
            .append(ResponseEvent::lifecycle(
                claim.response_id.clone(),
                ResponseEventKind::Failed,
                response,
            ))
            .await;
        let _ = deps
            .event_log
            .close(&claim.response_id, now, deps.retain_after_terminal_ms)
            .await;
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
