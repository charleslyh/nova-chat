use std::collections::HashMap;

use nova_responses::MetricsSink;
use parking_lot::Mutex;

pub struct MemMetrics {
    inner: Mutex<HashMap<String, u64>>,
}

impl MemMetrics {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for MemMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl MemMetrics {
    /// Readback for test assertions. Kept off the `MetricsSink` trait: the port
    /// is write-only, since a real backend cannot answer counter reads.
    #[allow(dead_code)]
    pub fn get(&self, name: &str) -> u64 {
        self.inner.lock().get(name).copied().unwrap_or(0)
    }
}

impl MetricsSink for MemMetrics {
    fn incr(&self, name: &str, value: u64) {
        *self.inner.lock().entry(name.to_string()).or_insert(0) += value;
    }
}
