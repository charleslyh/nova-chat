//! Automated verification harness: Trace · Oracle · L1/L2 scenarios.

mod l2;
mod oracle;
mod scenario;
mod trace;

pub use l2::run_l2_dir;
pub use oracle::{builtin, run_oracles, Oracle, Verdict};
pub use scenario::run_l1_dir;
pub use trace::{Trace, TraceEvent};
