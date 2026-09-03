//! Real wall-clock, the production counterpart to `adapters_mem`'s virtual clock.
//!
//! `MemClock` starts at 0 and only advances when a test calls `advance` — it is
//! the test clock. The sql backend must never use it: `created_at` / `expires_at`
//! would be virtual, and the sweeper's `reap(now, …)` would compare a real
//! heartbeat timestamp against a `now` of 0 and never time anything out.

use async_trait::async_trait;
use nova_responses_core::Clock;

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

#[async_trait]
impl Clock for SystemClock {
    async fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    async fn sleep_until_ms(&self, deadline_ms: u64) {
        let now = self.now_ms().await;
        if deadline_ms > now {
            tokio::time::sleep(std::time::Duration::from_millis(deadline_ms - now)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn now_ms_is_a_real_monotonic_wall_clock() {
        let clock = SystemClock;
        let a = clock.now_ms().await;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let b = clock.now_ms().await;
        // Not the virtual clock's frozen 0: it advances with wall time.
        assert!(b > a, "wall clock must advance: {a} -> {b}");
        assert!(b > 1_700_000_000_000, "must be a real epoch-millis value, got {b}");
    }
}
