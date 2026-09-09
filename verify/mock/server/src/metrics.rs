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

impl MetricsSink for MemMetrics {
    fn incr(&self, name: &str, value: u64) {
        *self.inner.lock().entry(name.to_string()).or_insert(0) += value;
    }

    fn get(&self, name: &str) -> u64 {
        self.inner.lock().get(name).copied().unwrap_or(0)
    }
}
