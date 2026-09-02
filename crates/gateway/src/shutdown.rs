//! Graceful shutdown.
//!
//! This is the single highest-value reliability measure in the design (D21).
//! Rolling deploys are the most frequent cause of in-flight loss, and unlike a
//! crash they are entirely predictable — so they can be made free:
//!
//! 1. stop accepting new work (creation returns 503; reads and subscriptions
//!    continue, so existing subscribers are not disturbed)
//! 2. wait for in-flight responses to finish, up to `drain_timeout_ms`
//! 3. exit
//!
//! Without this, one deploy across N nodes discards every in-flight response at
//! once — measurably worse than the unplanned crash rate it is compared against.

use std::time::Duration;

use tokio::signal;
use tracing::info;

use crate::state::AppState;

/// Resolves once the process should stop serving.
pub async fn drain(state: AppState) {
    wait_for_signal().await;

    state.stop_accepting();
    info!(
        node_tag = state.cfg.node_tag.as_str(),
        drain_timeout_ms = state.cfg.drain_timeout_ms,
        "shutdown signal received; refusing new responses and draining"
    );
    state.metrics.incr("drain_started", 1).await;

    let deadline =
        tokio::time::Instant::now() + Duration::from_millis(state.cfg.drain_timeout_ms);
    let mut last_reported = usize::MAX;

    loop {
        let in_flight = state.ledger.in_flight().await.unwrap_or(0);
        if in_flight == 0 {
            info!("drain complete: no in-flight responses");
            return;
        }
        if in_flight != last_reported {
            info!(in_flight, "draining");
            last_reported = in_flight;
        }
        if tokio::time::Instant::now() >= deadline {
            // Deliberate: a single very long generation must not block a deploy
            // indefinitely. The remainder is failed by the next node's startup
            // orphan reclaim (INV-45), so it fails explicitly rather than hanging.
            info!(
                in_flight,
                "drain budget exhausted; remaining responses will be reclaimed on restart"
            );
            state.metrics.incr("drain_timeouts", 1).await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        let mut term = signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = signal::ctrl_c().await;
    }
}
