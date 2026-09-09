/// Thin metrics sink for assertions (OR-3). Full Prometheus deferred to iteration 6.
///
/// Synchronous: a metrics sink is a process-local counter with no I/O, so there
/// is nothing to await. Forcing `async` here would make every caller pay an
/// executor hop that buys nothing.
pub trait MetricsSink: Send + Sync {
    fn incr(&self, name: &str, value: u64);
    fn get(&self, name: &str) -> u64;
}
