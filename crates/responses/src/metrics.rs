//! Minimal in-process counter sink.
//!
//! `MetricsSink` is a thin assertion surface (OR-3); full metrics export is
//! deferred. This default implementation lets the production gateway assemble
//! without borrowing a storage adapter's metrics, while the harness injects its
//! own (or this same one) to assert counters in tests.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use crate::MetricsSink;

#[derive(Default)]
pub struct CountingMetrics {
    inner: Mutex<HashMap<String, u64>>,
}

#[async_trait]
impl MetricsSink for CountingMetrics {
    async fn incr(&self, name: &str, value: u64) {
        *self
            .inner
            .lock()
            .expect("metrics lock poisoned")
            .entry(name.to_string())
            .or_insert(0) += value;
    }

    async fn get(&self, name: &str) -> u64 {
        self.inner
            .lock()
            .expect("metrics lock poisoned")
            .get(name)
            .copied()
            .unwrap_or(0)
    }
}
