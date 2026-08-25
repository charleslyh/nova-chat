//! Production ports. Signatures encode invariants. No implementations here.
//! Must not include FaultInjector (verification-only).

mod store;
mod ledger;
mod idempotency;
mod stream;
mod clock;
mod sandbox;
mod metrics;

pub use clock::Clock;
pub use idempotency::{GateError, IdempotencyGate, Reservation};
pub use ledger::{CapacityLedger, LedgerError};
pub use metrics::MetricsSink;
pub use sandbox::{PolicySandbox, SandboxError, SandboxInput};
pub use store::{
    CapacityAssertion, ClaimOutcome, FilterExpr, StoreError, TaskRecord, TaskStore,
};
pub use stream::{StreamChannel, StreamError};
