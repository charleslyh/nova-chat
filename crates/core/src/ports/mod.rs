//! Domain ports (traits). Implementations live in `crates/adapters/*`.

mod clock;
mod context;
mod event_log;
mod integrity;
mod ledger;
mod metrics;

pub use clock::Clock;
pub use context::{ContextError, ContextStore};
pub use event_log::{EventLogError, ResponseEventLog};
pub use integrity::{ContentIntegrity, IntegrityError};
pub use ledger::{
    AbortedClaim, ClaimedResponse, CreateOutcome, LedgerError, ResponseLedger,
};
pub use metrics::MetricsSink;
