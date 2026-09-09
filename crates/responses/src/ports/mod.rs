//! Domain ports (traits). Implementations live in the storage adapters (mock
//! reference in `verify/mock`, or an external integrator's own backend).

mod conversation;
mod event_log;
mod integrity;
mod ledger;
mod metrics;
mod store_error;

pub use conversation::{ConversationError, ConversationStore, TurnCommit};
pub use event_log::{EventLogError, ResponseEventLog};
pub use integrity::{ContentIntegrity, IntegrityError};
pub use ledger::{AbortedClaim, ClaimedResponse, CreateOutcome, LedgerError, ResponseLedger};
pub use metrics::MetricsSink;
pub use store_error::StoreError;
