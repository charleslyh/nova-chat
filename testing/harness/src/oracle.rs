//! Oracles: judge an entire Trace for cross-step invariants.

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

pub fn builtin(id: &str) -> Option<Box<dyn Oracle>> {
    Some(match id {
        "SeqMonotonic" => Box::new(SeqMonotonic),
        "SingleClaimPerAttempt" => Box::new(SingleClaimPerAttempt),
        "SubmittedTurnsTerminal" => Box::new(SubmittedTurnsTerminal),
        "NoSilentGap" => Box::new(NoSilentGap),
        "StaleAppendRejected" => Box::new(StaleAppendRejected),
        "IdempotentSameTurn" => Box::new(IdempotentSameTurn),
        _ => return None,
    })
}

pub struct SeqMonotonic;
impl Oracle for SeqMonotonic {
    fn id(&self) -> &'static str {
        "SeqMonotonic"
    }
    fn covers(&self) -> &'static [&'static str] {
        &["INV-11", "CR-5"]
    }
    fn judge(&self, trace: &Trace) -> Verdict {
        use std::collections::HashMap;
        let mut last: HashMap<uuid::Uuid, u64> = HashMap::new();
        for e in &trace.events {
            if let TraceEvent::StreamAppended {
                session_id, seq, ..
            } = e
            {
                let prev = last.get(session_id).copied().unwrap_or(0);
                if *seq <= prev {
                    return Verdict::Fail {
                        evidence: format!(
                            "session {session_id} seq {seq} not > previous {prev}"
                        ),
                    };
                }
                last.insert(*session_id, *seq);
            }
        }
        Verdict::Pass
    }
}

pub struct SingleClaimPerAttempt;
impl Oracle for SingleClaimPerAttempt {
    fn id(&self) -> &'static str {
        "SingleClaimPerAttempt"
    }
    fn covers(&self) -> &'static [&'static str] {
        &["CR-1", "INV-1"]
    }
    fn judge(&self, trace: &Trace) -> Verdict {
        use std::collections::HashMap;
        let mut wins: HashMap<(uuid::Uuid, u64), u32> = HashMap::new();
        for e in &trace.events {
            if let TraceEvent::TurnClaimed {
                turn_id, attempt, ..
            } = e
            {
                *wins.entry((*turn_id, *attempt)).or_insert(0) += 1;
            }
        }
        for ((turn, att), n) in wins {
            if n > 1 {
                return Verdict::Fail {
                    evidence: format!("turn {turn} attempt {att} claimed {n} times"),
                };
            }
        }
        Verdict::Pass
    }
}

pub struct SubmittedTurnsTerminal;
impl Oracle for SubmittedTurnsTerminal {
    fn id(&self) -> &'static str {
        "SubmittedTurnsTerminal"
    }
    fn covers(&self) -> &'static [&'static str] {
        &["CR-6", "INV-35"]
    }
    fn judge(&self, trace: &Trace) -> Verdict {
        use std::collections::HashSet;
        let mut submitted = HashSet::new();
        let mut terminal = HashSet::new();
        for e in &trace.events {
            match e {
                TraceEvent::TurnSubmitted {
                    turn_id,
                    outcome,
                    ..
                } if outcome == "accepted" || outcome == "duplicate" => {
                    submitted.insert(*turn_id);
                }
                TraceEvent::TurnTerminal { turn_id, .. } => {
                    terminal.insert(*turn_id);
                }
                _ => {}
            }
        }
        for t in &submitted {
            if !terminal.contains(t) {
                return Verdict::Fail {
                    evidence: format!("turn {t} submitted but not terminal in trace"),
                };
            }
        }
        Verdict::Pass
    }
}

pub struct NoSilentGap;
impl Oracle for NoSilentGap {
    fn id(&self) -> &'static str {
        "NoSilentGap"
    }
    fn covers(&self) -> &'static [&'static str] {
        &["INV-14", "CR-4"]
    }
    fn judge(&self, trace: &Trace) -> Verdict {
        use std::collections::HashMap;
        let mut earliest: HashMap<uuid::Uuid, u64> = HashMap::new();
        for e in &trace.events {
            match e {
                TraceEvent::StreamTrimmed {
                    session_id,
                    new_earliest,
                    ..
                } => {
                    earliest.insert(*session_id, *new_earliest);
                }
                TraceEvent::StreamRead {
                    session_id,
                    from_seq,
                    gap,
                    ..
                } => {
                    if let Some(ear) = earliest.get(session_id) {
                        if *from_seq < *ear && !*gap {
                            return Verdict::Fail {
                                evidence: format!(
                                    "session {session_id} read from_seq={from_seq} < earliest={ear} without gap=true"
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

pub struct StaleAppendRejected;
impl Oracle for StaleAppendRejected {
    fn id(&self) -> &'static str {
        "StaleAppendRejected"
    }
    fn covers(&self) -> &'static [&'static str] {
        &["CR-3", "INV-6"]
    }
    fn judge(&self, trace: &Trace) -> Verdict {
        let rejected = trace.events.iter().any(|e| {
            matches!(
                e,
                TraceEvent::StreamAppendRejected { reason, .. } if reason == "stale_attempt"
            )
        });
        if rejected {
            Verdict::Pass
        } else {
            Verdict::Fail {
                evidence: "expected at least one stale_attempt append rejection in trace".into(),
            }
        }
    }
}

pub struct IdempotentSameTurn;
impl Oracle for IdempotentSameTurn {
    fn id(&self) -> &'static str {
        "IdempotentSameTurn"
    }
    fn covers(&self) -> &'static [&'static str] {
        &["CR-2", "INV-2"]
    }
    fn judge(&self, trace: &Trace) -> Verdict {
        use std::collections::HashMap;
        let mut by_key: HashMap<String, uuid::Uuid> = HashMap::new();
        for e in &trace.events {
            if let TraceEvent::TurnSubmitted {
                key,
                turn_id,
                outcome,
                ..
            } = e
            {
                if outcome == "accepted" || outcome == "duplicate" {
                    if let Some(prev) = by_key.get(key) {
                        if prev != turn_id {
                            return Verdict::Fail {
                                evidence: format!(
                                    "idempotency key {key} mapped to different turns {prev} vs {turn_id}"
                                ),
                            };
                        }
                    } else {
                        by_key.insert(key.clone(), *turn_id);
                    }
                }
            }
        }
        let has_dup = trace.events.iter().any(|e| {
            matches!(
                e,
                TraceEvent::TurnSubmitted { outcome, .. } if outcome == "duplicate"
            )
        });
        if has_dup {
            Verdict::Pass
        } else {
            Verdict::Fail {
                evidence: "expected a duplicate submit outcome in trace".into(),
            }
        }
    }
}

pub fn run_oracles(trace: &Trace, names: &[String]) -> anyhow::Result<()> {
    for name in names {
        let Some(o) = builtin(name) else {
            anyhow::bail!("unknown oracle: {name}");
        };
        match o.judge(trace) {
            Verdict::Pass => {}
            Verdict::Fail { evidence } => {
                anyhow::bail!(
                    "oracle {} failed: {evidence} (trace={:?})",
                    o.id(),
                    trace.path()
                );
            }
        }
    }
    Ok(())
}
