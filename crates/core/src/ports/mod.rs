//! Domain ports (traits). Implementations live in `crates/adapters/*`.

mod context;
mod conversation;
mod event_log;
mod integrity;
mod ledger;
mod metrics;

pub use context::{ContextError, ContextStore};
pub use conversation::{ConversationError, ConversationStore};
pub use event_log::{EventLogError, ResponseEventLog};
pub use integrity::{ContentIntegrity, IntegrityError};
pub use ledger::{AbortedClaim, ClaimedResponse, CreateOutcome, LedgerError, ResponseLedger};
pub use metrics::MetricsSink;
