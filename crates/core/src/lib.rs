//! Domain types and protocols for Session-level replayable streams.

mod error;
mod events;
mod ids;
mod ports;
mod reconnect;
mod snapshot;

pub use error::DomainError;
pub use events::{EventKind, StreamEvent};
pub use ids::{AgentId, Attempt, IdempotencyKey, SessionId, TurnId};
pub use reconnect::JitteredBackoff;
pub use snapshot::{Bubble, SessionSnapshot};

pub use ports::{
    ClaimedTurn, Clock, MetaError, MetaStore, MetricsSink, SessionLock, SnapshotError,
    SnapshotStore, StreamChannel, StreamError, StreamGap, SubmitOutcome, TurnRecord, TurnStatus,
};
