use crate::trace::{Trace, TraceEvent};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Fail { evidence: String },
}

pub trait Oracle: Send + Sync {
    fn id(&self) -> &'static str;
    fn covers(&self) -> &'static [&'static str];
    fn judge(&self, trace: &Trace) -> Verdict;
}

pub struct NoDoubleClaim;
impl Oracle for NoDoubleClaim {
    fn id(&self) -> &'static str {
        "NoDoubleClaim"
    }
    fn covers(&self) -> &'static [&'static str] {
        &["CR-1"]
    }
    fn judge(&self, trace: &Trace) -> Verdict {
        use std::collections::HashMap;
        let mut wins: HashMap<(nova_core::TaskId, u64), u32> = HashMap::new();
        for e in &trace.events {
            if let TraceEvent::Claimed {
                task, attempt, ..
            } = e
            {
                *wins.entry((*task, attempt.0)).or_insert(0) += 1;
            }
        }
        for ((task, att), n) in wins {
            if n > 1 {
                return Verdict::Fail {
                    evidence: format!("task {task:?} attempt {att} claimed {n} times"),
                };
            }
        }
        Verdict::Pass
    }
}

pub struct AllTerminal;
impl Oracle for AllTerminal {
    fn id(&self) -> &'static str {
        "AllTerminal"
    }
    fn covers(&self) -> &'static [&'static str] {
        &["CR-7", "INV-35"]
    }
    fn judge(&self, trace: &Trace) -> Verdict {
        use std::collections::HashSet;
        let mut submitted = HashSet::new();
        let mut terminal = HashSet::new();
        for e in &trace.events {
            match e {
                TraceEvent::Submitted { task, .. } => {
                    submitted.insert(*task);
                }
                TraceEvent::Terminal { task, .. } => {
                    terminal.insert(*task);
                }
                _ => {}
            }
        }
        for t in &submitted {
            if !terminal.contains(t) {
                return Verdict::Fail {
                    evidence: format!("task {t:?} not terminal"),
                };
            }
        }
        Verdict::Pass
    }
}

pub struct NoOversell;
impl Oracle for NoOversell {
    fn id(&self) -> &'static str {
        "NoOversell"
    }
    fn covers(&self) -> &'static [&'static str] {
        &["CR-3", "CR-10"]
    }
    fn judge(&self, trace: &Trace) -> Verdict {
        use std::collections::HashMap;
        let mut used: HashMap<nova_core::WorkerId, i64> = HashMap::new();
        for e in &trace.events {
            if let TraceEvent::CapacityChanged { worker, delta, .. } = e {
                let v = used.entry(*worker).or_insert(0);
                *v += delta;
                if *v < 0 {
                    return Verdict::Fail {
                        evidence: format!("worker {worker:?} capacity went negative"),
                    };
                }
            }
        }
        Verdict::Pass
    }
}

pub struct StarvationBound {
    pub max_wait_ms: u64,
}
impl Oracle for StarvationBound {
    fn id(&self) -> &'static str {
        "StarvationBound"
    }
    fn covers(&self) -> &'static [&'static str] {
        &["FR-2.4"]
    }
    fn judge(&self, trace: &Trace) -> Verdict {
        use std::collections::HashMap;
        let mut submitted_at = HashMap::new();
        for e in &trace.events {
            match e {
                TraceEvent::Submitted { task, at_ms, .. } => {
                    submitted_at.insert(*task, *at_ms);
                }
                TraceEvent::Claimed { task, at_ms, .. } => {
                    if let Some(s) = submitted_at.get(task) {
                        if at_ms.saturating_sub(*s) > self.max_wait_ms {
                            return Verdict::Fail {
                                evidence: format!(
                                    "task {task:?} waited {}ms > {}",
                                    at_ms - s,
                                    self.max_wait_ms
                                ),
                            };
                        }
                    }
                }
                _ => {}
            }
        }
        Verdict::Pass
    }
}

pub fn builtin_oracles() -> Vec<Box<dyn Oracle>> {
    vec![
        Box::new(NoDoubleClaim),
        Box::new(AllTerminal),
        Box::new(NoOversell),
        Box::new(StarvationBound {
            max_wait_ms: 30 * 60 * 1000,
        }),
    ]
}

pub fn select_oracles(names: &[String]) -> Vec<Box<dyn Oracle>> {
    let all = builtin_oracles();
    if names.is_empty() {
        return all;
    }
    all.into_iter()
        .filter(|o| names.iter().any(|n| n == o.id()))
        .collect()
}
