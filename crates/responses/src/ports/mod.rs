//! Domain ports (traits) and the failures they report.
//!
//! Implementations live in the storage adapters (mock reference in `verify/mock`, or
//! an external integrator's own backend). Only traits and their error types belong
//! here — the *values* that travel through them ([`crate::TurnCommit`],
//! [`crate::ResponseRecord`]) live with the domain concept they describe.

mod conversation;
mod event_log;
mod integrity;
mod ledger;
mod metrics;
mod store_error;

pub use conversation::{
    ConversationError, ConversationEvents, ConversationRepo, ConversationSnapshots,
    ConversationStore, TurnLock,
};
pub use event_log::{EventLogError, ResponseEventLog};
pub use integrity::{ContentIntegrity, IntegrityError};
pub use ledger::{
    AbortedClaim, ClaimedResponse, CreateOutcome, LedgerError, ResponseClaimSource, ResponseIntake,
};
pub use metrics::{metric, MetricsSink};
pub use store_error::StoreError;
