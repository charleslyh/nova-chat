//! Real wall-clock, the production time source.
//!
//! The virtual counterpart is `adapters_mem`'s `MemClock`, which starts at 0 and
//! only advances when a test calls `advance`. Production must never use it:
//! `created_at` / `expires_at` would be virtual, and the sweeper's `reap(now, …)`
//! would compare a real heartbeat timestamp against a `now` of 0 and never time
//! anything out.

use std::sync::Arc;

/// Current wall-clock time in epoch milliseconds.
pub fn system_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The production timestamp function, matching the `Arc<dyn Fn() -> u64 + Send +
/// Sync>` seam that the orchestrator and services take for time.
pub fn system_now() -> Arc<dyn Fn() -> u64 + Send + Sync> {
    Arc::new(system_now_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_now_ms_is_a_real_epoch_millis_value() {
        let a = system_now_ms();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let b = system_now_ms();
        // Not the virtual clock's frozen 0: it advances with wall time.
        assert!(b > a, "wall clock must advance: {a} -> {b}");
        assert!(b > 1_700_000_000_000, "must be a real epoch-millis value, got {b}");
    }
}
