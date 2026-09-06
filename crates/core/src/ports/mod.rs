//! Domain ports (traits). Implementations live in `crates/adapters/*`.

mod clock;
mod completions;
mod context;
mod conversation;
mod event_log;
mod integrity;
mod ledger;
mod metrics;
mod session;
mod tool;

pub use clock::Clock;
pub use completions::{
    validate_outcome, CollectingSink, CompletionsRequestScheduler, CompletionsSink, SchedulerError,
    SinkError, SinkVerdict,
};
pub use context::{ContextError, ContextStore};
pub use conversation::{ConversationError, ConversationStore};
pub use event_log::{EventLogError, ResponseEventLog};
pub use integrity::{ContentIntegrity, IntegrityError};
pub use ledger::{
    AbortedClaim, ClaimedResponse, CreateOutcome, LedgerError, ResponseLedger,
};
pub use metrics::MetricsSink;
pub use session::{SessionError, SessionStore};
pub use tool::{NoopToolExecutor, ToolError, ToolExecutor};
