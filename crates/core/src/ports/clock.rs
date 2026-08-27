use async_trait::async_trait;

/// Production clock; test impl may advance (D15).
#[async_trait]
pub trait Clock: Send + Sync {
    async fn now_ms(&self) -> u64;
    async fn sleep_until_ms(&self, deadline_ms: u64);
}
