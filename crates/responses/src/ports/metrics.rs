use async_trait::async_trait;

/// Thin metrics sink for assertions (OR-3). Full Prometheus deferred to iteration 6.
#[async_trait]
pub trait MetricsSink: Send + Sync {
    async fn incr(&self, name: &str, value: u64);
    async fn get(&self, name: &str) -> u64;
}
