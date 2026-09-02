//! Automated verification harness: Trace · Oracle · L1/L2/L3 scenarios.

mod l2;
mod l3;
mod oracle;
mod scenario;
mod trace;

pub use l2::run_l2_dir;
pub use l3::{run_l3_dir, sql_ports_from_env, L3Availability};
pub use oracle::{all_ids, builtin, run_oracles, Oracle, Verdict};
pub use scenario::run_l1_dir;
pub use trace::{Trace, TraceEvent};
