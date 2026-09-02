//! In-memory adapters for the response service ports.
//!
//! Purpose: give L0–L2 a Docker-free substrate (D17). The sql adapter is the
//! production carrier; nothing here is durable or shared between processes.

mod clock;
mod context;
mod event_log;
mod ledger;
mod metrics;
mod store;

pub use clock::MemClock;
pub use context::MemContextStore;
pub use event_log::MemResponseEventLog;
pub use ledger::MemResponseLedger;
pub use metrics::MemMetrics;
pub use store::MemStore;

use std::sync::Arc;

use nova_responses_core::{ContentIntegrity, HmacSha256Integrity};

/// Capacity and retention knobs, mirroring the gateway configuration so tests
/// can exercise eviction and expiry without waiting for production-scale
/// numbers.
#[derive(Debug, Clone, Copy)]
pub struct MemWorldConfig {
    pub events_per_response: usize,
    pub max_logs: usize,
    pub max_records: usize,
    pub pending_limit: usize,
    pub verify_integrity: bool,
}

impl Default for MemWorldConfig {
    fn default() -> Self {
        Self {
            events_per_response: 20_000,
            max_logs: 100_000,
            max_records: 100_000,
            pending_limit: 10_000,
            verify_integrity: true,
        }
    }
}

#[derive(Clone)]
pub struct MemWorld {
    /// Shared state behind both the ledger and the context store, so their
    /// writes are atomic with respect to each other (D21 ①).
    pub store: Arc<MemStore>,
    pub ledger: Arc<MemResponseLedger>,
    pub event_log: Arc<MemResponseEventLog>,
    pub context: Arc<MemContextStore>,
    pub integrity: Option<Arc<dyn ContentIntegrity>>,
    pub clock: Arc<MemClock>,
    pub metrics: Arc<MemMetrics>,
}

impl MemWorld {
    pub fn new() -> Self {
        Self::with_config(MemWorldConfig::default())
    }

    pub fn with_config(cfg: MemWorldConfig) -> Self {
        let store = Arc::new(MemStore::new());
        store.set_pending_limit(cfg.pending_limit);
        store.set_max_records(cfg.max_records);

        let ledger = Arc::new(MemResponseLedger::new(store.clone()));
        let event_log = Arc::new(MemResponseEventLog::with_capacity(
            ledger.clone(),
            cfg.events_per_response,
            cfg.max_logs,
        ));

        // A fixed test key: real deployments read it from the environment and
        // fail startup when absent (INV-44). Using a literal here is safe
        // because this adapter is verification-only.
        let integrity: Option<Arc<dyn ContentIntegrity>> = if cfg.verify_integrity {
            Some(Arc::new(
                HmacSha256Integrity::from_key(b"mem-adapter-test-key-0123456789")
                    .expect("static test key is valid"),
            ))
        } else {
            None
        };

        let context = Arc::new(match &integrity {
            Some(i) => MemContextStore::with_integrity(store.clone(), i.clone()),
            None => MemContextStore::new(store.clone()),
        });

        Self {
            store,
            ledger,
            event_log,
            context,
            integrity,
            clock: Arc::new(MemClock::new()),
            metrics: Arc::new(MemMetrics::new()),
        }
    }
}

impl Default for MemWorld {
    fn default() -> Self {
        Self::new()
    }
}
