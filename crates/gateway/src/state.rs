//! Shared application state.
//!
//! Every port is held as a trait object: the ingress layer must not know which
//! backend is mounted. That is what lets the mem→sql switch happen with zero
//! changes here, including the automatic retirement of chain affinity routing
//! once `context.is_shared()` becomes true (D21).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use nova_responses_core::{
    Clock, ContextStore, MetricsSink, ResponseEventLog, ResponseLedger,
};

use crate::auth::KeyTable;
use crate::config::Config;

#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<Config>,
    pub ledger: Arc<dyn ResponseLedger>,
    pub event_log: Arc<dyn ResponseEventLog>,
    pub context: Arc<dyn ContextStore>,
    pub clock: Arc<dyn Clock>,
    pub metrics: Arc<dyn MetricsSink>,
    pub keys: Arc<KeyTable>,
    pub http: reqwest::Client,
    /// Cleared on SIGTERM so creation is refused while in-flight work drains
    /// (FR-34). Reads and subscriptions keep serving.
    pub accepting: Arc<AtomicBool>,
    /// Wakes the execution engine after a generation is accepted.
    ///
    /// A notification rather than a poll interval: sync mode waits for a terminal
    /// state, so polling latency would be added directly to every caller's
    /// first-token time.
    pub work_ready: Arc<tokio::sync::Notify>,
}

impl AppState {
    pub fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::SeqCst)
    }

    pub fn stop_accepting(&self) {
        self.accepting.store(false, Ordering::SeqCst);
    }

    /// Signal that this node has work to run.
    pub fn notify_work(&self) {
        self.work_ready.notify_one();
    }

    pub async fn now_ms(&self) -> u64 {
        self.clock.now_ms().await
    }

    /// Whether content reads may go straight to the store.
    ///
    /// False for the in-memory backend, which lives inside one process, so the
    /// ingress layer forwards to the owning node instead. This is a temporary
    /// measure that disappears with shared storage — unlike in-flight event
    /// routing, which is permanent.
    pub fn content_is_shared(&self) -> bool {
        self.context.is_shared()
    }
}
