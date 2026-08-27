//! Client reconnect backoff (INV-33 / OR-1).
//!
//! Formula (aligned with design): `min(base * 2^n, max) * jitter`,
//! where `jitter ∈ [jitter_min, jitter_max]` (typically `[0.5, 1.5]`).

use std::time::Duration;

/// Jittered exponential backoff for stream / API reconnects.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct JitteredBackoff {
    pub base: Duration,
    pub max: Duration,
    pub jitter_min: f64,
    pub jitter_max: f64,
}

impl Default for JitteredBackoff {
    fn default() -> Self {
        Self {
            base: Duration::from_millis(200),
            max: Duration::from_secs(30),
            jitter_min: 0.5,
            jitter_max: 1.5,
        }
    }
}

impl JitteredBackoff {
    /// `attempt` is 0-based (first retry = 0 → ~base).
    /// `rand_unit` must be in `[0.0, 1.0]`; maps linearly into `[jitter_min, jitter_max]`.
    pub fn delay(&self, attempt: u32, rand_unit: f64) -> Duration {
        let shift = attempt.min(20);
        let exp_ms = self
            .base
            .as_millis()
            .saturating_mul(1u128 << shift)
            .min(self.max.as_millis());
        let u = rand_unit.clamp(0.0, 1.0);
        let jitter =
            self.jitter_min + (self.jitter_max - self.jitter_min) * u;
        let ms = ((exp_ms as f64) * jitter).round().max(0.0) as u64;
        Duration::from_millis(ms)
    }

    /// Inclusive bounds for a given attempt (jitter extremes).
    pub fn delay_bounds(&self, attempt: u32) -> (Duration, Duration) {
        (
            self.delay(attempt, 0.0),
            self.delay(attempt, 1.0),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_grows_then_caps() {
        let b = JitteredBackoff::default();
        let d0 = b.delay(0, 1.0).as_millis(); // max jitter
        let d1 = b.delay(1, 1.0).as_millis();
        let d2 = b.delay(2, 1.0).as_millis();
        assert!(d1 > d0);
        assert!(d2 > d1);
        let high = b.delay(20, 1.0);
        assert!(high <= b.max.saturating_mul(2)); // jitter up to 1.5
        assert!(high.as_millis() <= (30_000.0 * 1.5) as u128);
    }

    #[test]
    fn jitter_stays_in_band() {
        let b = JitteredBackoff::default();
        let (lo, hi) = b.delay_bounds(3);
        assert!(lo.as_millis() >= (200 * 8) as u128 / 2); // 0.5 * 1600
        assert!(hi.as_millis() <= (200 * 8) as u128 * 3 / 2 + 1); // 1.5 * 1600
        for u in [0.0, 0.25, 0.5, 0.75, 1.0] {
            let d = b.delay(3, u);
            assert!(d >= lo && d <= hi);
        }
    }

    #[test]
    fn inv33_requires_jitter_spread() {
        let b = JitteredBackoff::default();
        let (lo, hi) = b.delay_bounds(2);
        assert!(hi > lo, "INV-33: reconnect delay must include jitter spread");
    }
}
