//! In-memory implementations of all production ports (iteration 0).
//! PolicySandbox interpreter lives here — not in nova-matcher.

mod store;
mod ledger;
mod idempotency;
mod stream;
mod clock;
mod sandbox;
mod metrics;

pub use clock::MemClock;
pub use idempotency::MemIdempotencyGate;
pub use ledger::MemCapacityLedger;
pub use metrics::MemMetrics;
pub use sandbox::MemPolicySandbox;
pub use store::MemTaskStore;
pub use stream::MemStreamChannel;

use std::sync::Arc;

/// Bundle of all mem adapters sharing nothing illegally between store and stream.
#[derive(Clone)]
pub struct MemWorld {
    pub store: Arc<MemTaskStore>,
    pub ledger: Arc<MemCapacityLedger>,
    pub gate: Arc<MemIdempotencyGate>,
    pub stream: Arc<MemStreamChannel>,
    pub clock: Arc<MemClock>,
    pub sandbox: Arc<MemPolicySandbox>,
    pub metrics: Arc<MemMetrics>,
}

impl MemWorld {
    pub fn new() -> Self {
        Self {
            store: Arc::new(MemTaskStore::new()),
            ledger: Arc::new(MemCapacityLedger::new()),
            gate: Arc::new(MemIdempotencyGate::new()),
            stream: Arc::new(MemStreamChannel::new()),
            clock: Arc::new(MemClock::new()),
            sandbox: Arc::new(MemPolicySandbox::new()),
            metrics: Arc::new(MemMetrics::new()),
        }
    }
}

impl Default for MemWorld {
    fn default() -> Self {
        Self::new()
    }
}
