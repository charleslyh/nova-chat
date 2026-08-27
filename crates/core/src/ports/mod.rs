//! Domain protocols (traits). Implementations live in `crates/adapters/*` (e.g. `adapters-mem`).

mod clock;
mod meta;
mod metrics;
mod snapshot;
mod stream;

pub use clock::Clock;
pub use meta::{
    ClaimedTurn, MetaError, MetaStore, SessionLock, SubmitOutcome, TurnRecord, TurnStatus,
};
pub use metrics::MetricsSink;
pub use snapshot::{SnapshotError, SnapshotStore};
pub use stream::{StreamChannel, StreamError, StreamGap};
