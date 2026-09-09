//! The clock, as a trait.
//!
//! Every layer that stamps a timestamp needs one. Time is injected rather than read from
//! the system so retention windows, heartbeat timeouts and expiry are testable under a
//! virtual clock (D15).
//!
//! This used to be a value type wrapping `Arc<dyn Fn() -> u64 + Send + Sync>`, with the
//! test conveniences (`fixed`, `new(closure)`) living on the production type and the
//! virtual clock's controls (`advance` / `set`) living on a *separate* `MemClock` in the
//! verification crate, glued back in through a closure. That split one fact — the time —
//! across two objects, and forced every dependency struct to spell out an anonymous
//! function type that could not be `Debug`-printed or meaningfully named.
//!
//! The trait makes the seam explicit. The trait itself carries only what every consumer
//! needs — "what time is it" — and nothing about how to change it: production mounts
//! [`SystemClock`], and a verification build mounts its own clock that implements this
//! same trait and *adds* the controls (`advance`, `set`) it needs. Those controls belong
//! on the concrete type, not on the trait, so production code can never reach them.

use std::time::{SystemTime, UNIX_EPOCH};

/// A source of wall-clock milliseconds since the Unix epoch.
///
/// Object-safe on purpose: consumers hold `Arc<dyn Clock>`, so a test can hand in a
/// virtual clock and production a real one without either knowing which it got.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

/// The real wall clock. Production mounts this.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_system_clock_is_after_the_epoch() {
        assert!(SystemClock.now_ms() > 1_600_000_000_000);
    }

    #[test]
    fn the_system_clock_is_a_clock() {
        // The trait object form is what every consumer actually holds.
        let clock: &dyn Clock = &SystemClock;
        assert!(clock.now_ms() > 1_600_000_000_000);
    }
}
