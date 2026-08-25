//! Domain types and pure functions. Zero I/O — must not depend on `nova-ports`.

mod ids;
mod spec;
mod state;
mod events;
mod error;

pub use error::DomainError;
pub use events::{EventKind, OutputEvent};
pub use ids::{Attempt, IdempotencyKey, ObserverId, TaskId, WorkerId};
pub use spec::{CapacityNeed, Priority, TaskKind, TaskSpec, WorkerProfile};
pub use state::{TaskState, transition};
