use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use nova_responses_core::Clock;
use tokio::sync::Notify;

pub struct MemClock {
    now_ms: AtomicU64,
    notify: Notify,
}

impl MemClock {
    pub fn new() -> Self {
        Self {
            now_ms: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }

    pub fn advance(&self, delta_ms: u64) {
        self.now_ms.fetch_add(delta_ms, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    pub fn set(&self, ms: u64) {
        self.now_ms.store(ms, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

impl Default for MemClock {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Clock for MemClock {
    async fn now_ms(&self) -> u64 {
        self.now_ms.load(Ordering::SeqCst)
    }

    async fn sleep_until_ms(&self, deadline_ms: u64) {
        loop {
            if self.now_ms.load(Ordering::SeqCst) >= deadline_ms {
                return;
            }
            self.notify.notified().await;
        }
    }
}
