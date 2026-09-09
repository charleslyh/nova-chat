/// Thin metrics sink (OR-3). Full Prometheus deferred to iteration 6.
///
/// Synchronous: a metrics sink is a process-local counter with no I/O, so there is
/// nothing to await. Forcing `async` here would make every caller pay an executor hop
/// that buys nothing.
///
/// Write-only on purpose: a real backend (Prometheus, OTel) cannot answer "what is the
/// current counter" for a name it only ever increments, so readback is not part of the
/// port. Tests that assert on counters use a concrete in-memory sink with its own
/// inherent read method.
///
/// **Counters only.** There is deliberately no `observe`, which is why the names below
/// are all things that happened rather than measurements: a counter fed a momentary
/// value (a chain depth, a queue length) produces a number no backend can interpret.
pub trait MetricsSink: Send + Sync {
    fn incr(&self, name: &str, value: u64);
}

/// The counter names this crate emits.
///
/// Constants rather than literals at each call site: a metric name is a contract with
/// whatever scrapes it, and a typo in a string literal is invisible until a dashboard
/// is quietly empty.
pub mod metric {
    pub const RESPONSES_CREATED: &str = "responses_created";
    pub const RESPONSES_CANCELLED: &str = "responses_cancelled";
    pub const RESPONSES_DELETED: &str = "responses_deleted";
    pub const RESPONSES_REAPED: &str = "responses_reaped";
    pub const CHAIN_RESOLVED: &str = "chain_resolved";
    pub const CONVERSATIONS_CREATED: &str = "conversations_created";
    pub const CONVERSATIONS_DELETED: &str = "conversations_deleted";
    pub const CONVERSATION_STALE_LOCKS_RELEASED: &str = "conversation_stale_locks_released";
    pub const EVENT_LOGS_SWEPT: &str = "event_logs_swept";
    pub const TENANT_PURGES: &str = "tenant_purges";
}
