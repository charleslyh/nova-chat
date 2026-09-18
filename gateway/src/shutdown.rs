//! Graceful shutdown.
//!
//! 1. stop accepting new work (creation returns 503; reads and subscriptions
//!    continue, so existing subscribers are not disturbed)
//! 2. serve out the drain window, then exit
//!
//! The window used to be cut short by polling the ledger's in-flight count
//! (FR-34). That count is a task-management concern owned by the host's
//! execution system, not by the record book, so it left the port contract:
//! drain supervision now belongs to the business side, and the gateway holds
//! a fixed grace period instead. Executors that outlive the process are
//! recovered by the reap path (INV-45) on the next node's sweep.

use tokio::signal;
use tracing::info;

use crate::state::AppState;

/// Resolves once the process should stop serving.
pub async fn drain(state: AppState) {
    wait_for_signal().await;

    state.stop_accepting();
    info!(
        node_tag = state.responses_cfg().node_tag.as_str(),
        drain_window_ms = state.cfg.drain_timeout_ms,
        "shutdown signal received; refusing new responses and serving out the drain window"
    );
    state.metrics.incr("drain_started", 1);

    // Fixed grace: the window is bounded by configuration alone, since the
    // in-flight count is no longer the gateway's to observe. Work that outlives
    // it is failed by the next node's sweeper (INV-45), not lost silently.
    tokio::time::sleep(state.cfg.drain_timeout()).await;
    info!("drain window elapsed; shutting down");
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
