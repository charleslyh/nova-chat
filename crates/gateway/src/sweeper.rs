//! Background maintenance, merged into a single loop.
//!
//! Reaping lost claims, releasing expired event buffers and clearing expired
//! content all run on the same tick. Three separate loops would mean three
//! timers and three chances to forget one.

use std::time::Duration;

use nova_responses_core::{ResponseEvent, ResponseEventKind};
use tracing::warn;

use crate::state::AppState;

const TICK: Duration = Duration::from_secs(2);
/// Bounded per pass so one tick cannot turn into a long transaction.
const SWEEP_BATCH: usize = 500;

pub fn spawn(state: AppState) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(TICK).await;
            if let Err(e) = tick(&state).await {
                // Logged without payloads: only ids and codes ever reach the log
                // (SEC-9).
                warn!(error = %e, "sweeper tick failed");
            }
        }
    });
}

async fn tick(state: &AppState) -> anyhow::Result<()> {
    let now = state.now_ms().await;

    // 1. Reap claims whose holder stopped heartbeating. The ledger raises the
    //    attempt fence, so the dead holder cannot append afterwards (INV-6).
    let aborted = state
        .ledger
        .reap(now, state.cfg.heartbeat_ttl_ms)
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
        let _ = state
            .event_log
            .append(ResponseEvent::lifecycle(
                claim.response_id.clone(),
                ResponseEventKind::Failed,
                response,
            ))
            .await;
        let _ = state
            .event_log
            .close(&claim.response_id, now, state.cfg.retain_after_terminal_ms)
            .await;
        state.metrics.incr("responses_reaped", 1).await;
    }

    // 2. Release event buffers past their retention window.
    match state.event_log.sweep_expired(now).await {
        Ok(0) => {}
        Ok(n) => state.metrics.incr("event_logs_swept", n).await,
        Err(e) => warn!(error = %e, "event log sweep failed"),
    }

    // 3. Clear expired stored content (FR-22 / OR-5).
    match state.context.sweep_expired(now, SWEEP_BATCH).await {
        Ok(0) => {}
        Ok(n) => state.metrics.incr("content_expired", n).await,
        Err(e) => warn!(error = %e, "content sweep failed"),
    }

    Ok(())
}
