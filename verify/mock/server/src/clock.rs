use std::sync::atomic::{AtomicU64, Ordering};

use nova_responses::Clock;

/// A virtual clock that starts at 0 and only advances when the test calls
/// [`advance`](Self::advance) / [`set`](Self::set) (D15).
///
/// This is the verification-side implementation of the [`Clock`] trait: it answers
/// `now_ms` like any clock, and *adds* the controls a test needs. Production must never
/// mount it — `created_at` / `expires_at` would be virtual, and the sweeper would compare
/// real heartbeat timestamps against a frozen `0` and never time anything out. Production
/// mounts `nova_responses::SystemClock`.
pub struct MemClock {
    now_ms: AtomicU64,
}

impl MemClock {
    pub fn new() -> Self {
        Self {
            now_ms: AtomicU64::new(0),
        }
    }

    /// A clock frozen at `ms`, for tests that need no progression at all.
    pub fn fixed(ms: u64) -> Self {
        Self {
            now_ms: AtomicU64::new(ms),
        }
    }

    pub fn advance(&self, delta_ms: u64) {
        self.now_ms.fetch_add(delta_ms, Ordering::SeqCst);
    }

    pub fn set(&self, ms: u64) {
        self.now_ms.store(ms, Ordering::SeqCst);
    }
}

impl Clock for MemClock {
    fn now_ms(&self) -> u64 {
        self.now_ms.load(Ordering::SeqCst)
    }
}

impl Default for MemClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_virtual_clock_only_moves_when_told_to() {
        let clock = MemClock::new();
        assert_eq!(clock.now_ms(), 0);
        clock.advance(5);
        assert_eq!(clock.now_ms(), 5);
        clock.set(9);
        assert_eq!(clock.now_ms(), 9);
    }

    #[test]
    fn a_fixed_clock_does_not_move() {
        let clock = MemClock::fixed(7);
        assert_eq!(clock.now_ms(), 7);
        assert_eq!(clock.now_ms(), 7);
    }

    #[test]
    fn it_is_a_clock() {
        // The seam every consumer relies on: the verification clock is a drop-in `Clock`.
        let clock: &dyn Clock = &MemClock::fixed(42);
        assert_eq!(clock.now_ms(), 42);
    }
}
