//! Verification infrastructure: traces, oracles, scenarios, mock workers (L1+).

mod trace;
mod oracles;
mod scenario;
mod mock_worker;
mod fault;
mod coverage;

pub use coverage::{CoverageRegistry, KnownRequirement};
pub use fault::{FaultInjector, FaultKind};
pub use mock_worker::MockWorker;
pub use oracles::{Oracle, Verdict, builtin_oracles};
pub use scenario::{run_scenario_file, run_l1_dir, ScenarioSpec};
pub use trace::{Trace, TraceEvent};
