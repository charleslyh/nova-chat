//! The gateway's default metrics sink.
//!
//! A no-op: the gateway process has no metrics consumer of its own, and this keeps the
//! assembly identical whether or not an operator wires a real backend later (D25 — the
//! domain crate defines the port, the assembler supplies the implementation).
//!
//! This used to be a `CountingMetrics` with a `get` readback for test assertions, marked
//! `#[allow(dead_code)]`. A readback is a *verification* capability — a real backend
//! (Prometheus, OTel) cannot answer "what is the current counter" for a name it only ever
//! increments — so it belongs on the verification-side double (`MemMetrics` in
//! `verify/mock`), not in the production assembly crate. Stripped of the readback, a
//! counter that accumulates into a map nobody reads is just a no-op that costs memory, so
//! the honest default is to do nothing.

use nova_responses::ports::MetricsSink;

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopMetrics;

impl MetricsSink for NoopMetrics {
    fn incr(&self, _name: &str, _value: u64) {}
}
