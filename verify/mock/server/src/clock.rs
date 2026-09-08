use std::sync::atomic::{AtomicU64, Ordering};

/// A virtual clock that starts at 0 and only advances when the test calls
/// [`advance`](Self::advance) / [`set`](Self::set) (D15).
///
/// Production must never mount this: `created_at` / `expires_at` would be
/// virtual, and the sweeper would compare real heartbeat timestamps against a
/// frozen `0` and never time anything out. The production timestamp function is
/// `nova_responses::system_now`.
pub struct MemClock {
    now_ms: AtomicU64,
}

impl MemClock {
    pub fn new() -> Self {
        Self {
            now_ms: AtomicU64::new(0),
        }
    }

    pub fn advance(&self, delta_ms: u64) {
        self.now_ms.fetch_add(delta_ms, Ordering::SeqCst);
    }

    pub fn set(&self, ms: u64) {
        self.now_ms.store(ms, Ordering::SeqCst);
    }

    pub fn now_ms(&self) -> u64 {
        self.now_ms.load(Ordering::SeqCst)
    }
}

impl Default for MemClock {
    fn default() -> Self {
        Self::new()
    }
}
