//! In-process counter sink used when no real metrics backend is mounted.
//!
//! Lives here rather than in `nova-responses` because a concrete `MetricsSink`
//! is an assembly concern (D25): the domain crate defines the port, the gateway
//! supplies the default implementation.

use std::collections::HashMap;
use std::sync::Mutex;

use nova_responses::MetricsSink;

/// A process-local counter, the default when no metrics backend is wired. Test
/// harnesses can inject their own (or this one) to assert counters.
#[derive(Default)]
pub struct CountingMetrics {
    inner: Mutex<HashMap<String, u64>>,
}

impl CountingMetrics {
    /// Readback for test assertions. Kept off the `MetricsSink` trait: the port
    /// is write-only, since a real backend cannot answer counter reads.
    #[allow(dead_code)]
    pub fn get(&self, name: &str) -> u64 {
        self.inner
            .lock()
            .expect("metrics lock poisoned")
            .get(name)
            .copied()
            .unwrap_or(0)
    }
}

impl MetricsSink for CountingMetrics {
    fn incr(&self, name: &str, value: u64) {
        *self
            .inner
            .lock()
            .expect("metrics lock poisoned")
            .entry(name.to_string())
            .or_insert(0) += value;
    }
}
