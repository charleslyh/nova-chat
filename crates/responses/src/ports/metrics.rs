/// Thin metrics sink (OR-3). Full Prometheus deferred to iteration 6.
///
/// Synchronous: a metrics sink is a process-local counter with no I/O, so there
/// is nothing to await. Forcing `async` here would make every caller pay an
/// executor hop that buys nothing.
///
/// Write-only on purpose: a real backend (Prometheus, OTel) cannot answer "what
/// is the current counter" for a name it only ever increments, so readback is
/// not part of the port. Tests that assert on counters use a concrete in-memory
/// sink with its own inherent read method.
pub trait MetricsSink: Send + Sync {
    fn incr(&self, name: &str, value: u64);
}
