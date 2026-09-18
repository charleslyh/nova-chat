//! Oracles: judge an entire Trace for cross-step invariants.
//!
//! An oracle differs from a step assertion: it inspects the *whole* trace, so it
//! catches violations that no individual step could observe — for example a
//! sequence gap that only becomes visible when two appends are compared.

use std::collections::{HashMap, HashSet};

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

    /// Human-readable description of what this oracle needs to see.
    fn needs(&self) -> &'static str;

    /// Whether the trace actually contains anything this oracle judges.
    ///
    /// Required, not defaulted, because most oracles are written as "scan for a
    /// violation, otherwise pass" — which passes trivially on a trace that never
    /// exercised the property. `IntegrityVerified`, for instance, only fails on a
    /// recorded check that came back false, so a scenario that performs no
    /// integrity check at all would pass it while contributing CR-13 to the
    /// coverage figure. That is a false claim of verification, and it is exactly
    /// the kind that survives review.
    fn saw_relevant_data(&self, trace: &Trace) -> bool;
}

/// Count trace entries matching a predicate.
fn count(trace: &Trace, f: impl Fn(&TraceEvent) -> bool) -> usize {
    trace.events.iter().filter(|e| f(e)).count()
}

pub fn builtin(id: &str) -> Option<Box<dyn Oracle>> {
    Some(match id {
        "SequenceContiguous" => Box::new(SequenceContiguous),
        "SingleClaimPerAttempt" => Box::new(SingleClaimPerAttempt),
        "CreatedResponsesTerminal" => Box::new(CreatedResponsesTerminal),
        "ExpiredIsExplicit" => Box::new(ExpiredIsExplicit),
        "StaleAppendRejected" => Box::new(StaleAppendRejected),
        "IdempotentSameResponse" => Box::new(IdempotentSameResponse),
        "ChainBounded" => Box::new(ChainBounded),
        "NoSilentContentLoss" => Box::new(NoSilentContentLoss),
        "IntegrityVerified" => Box::new(IntegrityVerified),
        "UsageAccounted" => Box::new(UsageAccounted),
        "ChainClosure" => Box::new(ChainClosure),
        _ => return None,
    })
}

pub fn all_ids() -> &'static [&'static str] {
    &[
        "SequenceContiguous",
        "SingleClaimPerAttempt",
        "CreatedResponsesTerminal",
        "ExpiredIsExplicit",
        "StaleAppendRejected",
        "IdempotentSameResponse",
        "ChainBounded",
        "NoSilentContentLoss",
        "IntegrityVerified",
        "UsageAccounted",
        "ChainClosure",
    ]
}

/// Judge a trace against the named oracles. An unknown name is an error rather
/// than a silent skip — otherwise a typo in a scenario would quietly disable a
/// check.
pub fn run_oracles(trace: &Trace, names: &[String]) -> anyhow::Result<()> {
    for name in names {
        let Some(o) = builtin(name) else {
            anyhow::bail!("unknown oracle: {name}");
        };
        // A declared oracle that had nothing to judge is an error. The scenario
        // claims to substantiate this oracle's requirements — and the coverage
        // gate counts them — so passing vacuously would report verification that
        // never happened.
        if !o.saw_relevant_data(trace) {
            anyhow::bail!(
                "oracle {} was declared but the trace contains no {} to judge, so it \
                 would pass without checking anything. Either exercise the property \
                 in this scenario or remove the oracle (its `covers` list feeds the \
                 coverage gate). trace={:?}",
                o.id(),
                o.needs(),
                trace.path()
            );
        }
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

/// INV-11: sequence numbers within one response are 0-based and contiguous.
///
/// Strictly stronger than the previous "monotonic" check, which could not
/// distinguish `0,1,2` from `0,5,9`. Contiguity is what `starting_after=N`
/// depends on, and it is the precondition for having removed gap recovery
/// entirely.
pub struct SequenceContiguous;
impl Oracle for SequenceContiguous {
    fn id(&self) -> &'static str {
        "SequenceContiguous"
    }
    fn covers(&self) -> &'static [&'static str] {
        &["INV-11", "CR-5", "CR-4"]
    }
    fn needs(&self) -> &'static str {
        "appended events"
    }
    fn saw_relevant_data(&self, trace: &Trace) -> bool {
        count(trace, |e| matches!(e, TraceEvent::EventAppended { .. })) > 0
    }
    fn judge(&self, trace: &Trace) -> Verdict {
        let mut expected: HashMap<String, u64> = HashMap::new();
        for e in &trace.events {
            if let TraceEvent::EventAppended {
                response_id,
                sequence_number,
                ..
            } = e
            {
                let want = expected.entry(response_id.clone()).or_insert(0);
                if sequence_number != want {
                    return Verdict::Fail {
                        evidence: format!(
                            "response {response_id}: expected sequence {want}, got {sequence_number}"
                        ),
                    };
                }
                *want += 1;
            }
        }
        Verdict::Pass
    }
}

/// CR-1 / INV-1: one winner per attempt.
pub struct SingleClaimPerAttempt;
impl Oracle for SingleClaimPerAttempt {
    fn id(&self) -> &'static str {
        "SingleClaimPerAttempt"
    }
    fn covers(&self) -> &'static [&'static str] {
        &["CR-1", "INV-1"]
    }
    fn needs(&self) -> &'static str {
        "claims"
    }
    fn saw_relevant_data(&self, trace: &Trace) -> bool {
        count(trace, |e| matches!(e, TraceEvent::ResponseClaimed { .. })) > 0
    }
    fn judge(&self, trace: &Trace) -> Verdict {
        let mut wins: HashMap<(String, u64), u32> = HashMap::new();
        for e in &trace.events {
            if let TraceEvent::ResponseClaimed {
                response_id,
                attempt,
                ..
            } = e
            {
                *wins.entry((response_id.clone(), *attempt)).or_insert(0) += 1;
            }
        }
        for ((response, attempt), n) in wins {
            if n > 1 {
                return Verdict::Fail {
                    evidence: format!("response {response} attempt {attempt} claimed {n} times"),
                };
            }
        }
        Verdict::Pass
    }
}

/// CR-6 / INV-35: an accepted response always reaches a terminal state.
pub struct CreatedResponsesTerminal;
impl Oracle for CreatedResponsesTerminal {
    fn id(&self) -> &'static str {
        "CreatedResponsesTerminal"
    }
    fn covers(&self) -> &'static [&'static str] {
        &["CR-6", "INV-35"]
    }
    fn needs(&self) -> &'static str {
        "created responses"
    }
    fn saw_relevant_data(&self, trace: &Trace) -> bool {
        count(trace, |e| matches!(e, TraceEvent::ResponseCreated { .. })) > 0
    }
    fn judge(&self, trace: &Trace) -> Verdict {
        let mut created: HashSet<String> = HashSet::new();
        let mut terminal: HashSet<String> = HashSet::new();
        for e in &trace.events {
            match e {
                TraceEvent::ResponseCreated {
                    response_id,
                    outcome,
                    ..
                } if outcome == "accepted" => {
                    created.insert(response_id.clone());
                }
                TraceEvent::ResponseTerminal { response_id, .. } => {
                    terminal.insert(response_id.clone());
                }
                _ => {}
            }
        }
        let stuck: Vec<_> = created.difference(&terminal).cloned().collect();
        if stuck.is_empty() {
            Verdict::Pass
        } else {
            Verdict::Fail {
                evidence: format!("accepted but never terminal: {stuck:?}"),
            }
        }
    }
}

/// INV-40: an unavailable cursor is reported, never papered over.
///
/// Replaces the old gap oracle. The failure mode it guards against is subtly
/// different: previously a gap had to be *recoverable*; now it must be
/// *explicit*, because there is deliberately no recovery path.
pub struct ExpiredIsExplicit;
impl Oracle for ExpiredIsExplicit {
    fn id(&self) -> &'static str {
        "ExpiredIsExplicit"
    }
    fn covers(&self) -> &'static [&'static str] {
        &["INV-40", "CR-4", "FR-12"]
    }
    fn needs(&self) -> &'static str {
        "event reads"
    }
    fn saw_relevant_data(&self, trace: &Trace) -> bool {
        count(trace, |e| matches!(e, TraceEvent::EventRead { .. })) > 0
    }
    fn judge(&self, trace: &Trace) -> Verdict {
        for e in &trace.events {
            if let TraceEvent::EventRead {
                response_id,
                starting_after,
                count,
                expired,
                ..
            } = e
            {
                // A read that hit an evicted position must say so. Returning rows
                // *and* claiming expiry would mean partial data was served.
                if *expired && *count > 0 {
                    return Verdict::Fail {
                        evidence: format!(
                            "response {response_id} reported expiry at cursor {starting_after:?} \
                             yet returned {count} events"
                        ),
                    };
                }
            }
        }
        Verdict::Pass
    }
}

/// CR-3 / INV-6: a superseded attempt cannot append.
pub struct StaleAppendRejected;
impl Oracle for StaleAppendRejected {
    fn id(&self) -> &'static str {
        "StaleAppendRejected"
    }
    fn covers(&self) -> &'static [&'static str] {
        &["CR-3", "INV-6", "CR-7"]
    }
    fn needs(&self) -> &'static str {
        "appends or append rejections"
    }
    fn saw_relevant_data(&self, trace: &Trace) -> bool {
        count(trace, |e| matches!(e, TraceEvent::EventAppended { .. } | TraceEvent::EventAppendRejected { .. })) > 0
    }
    fn judge(&self, trace: &Trace) -> Verdict {
        // Highest attempt seen per response; an append below it must have been
        // rejected rather than accepted.
        let mut highest: HashMap<String, u64> = HashMap::new();
        for e in &trace.events {
            match e {
                TraceEvent::ResponseClaimed {
                    response_id,
                    attempt,
                    ..
                } => {
                    let entry = highest.entry(response_id.clone()).or_insert(0);
                    *entry = (*entry).max(*attempt);
                }
                TraceEvent::EventAppended {
                    response_id,
                    attempt: Some(attempt),
                    sequence_number,
                    ..
                } => {
                    if let Some(current) = highest.get(response_id) {
                        if attempt < current {
                            return Verdict::Fail {
                                evidence: format!(
                                    "response {response_id}: stale attempt {attempt} appended at \
                                     sequence {sequence_number} while {current} is current"
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

/// CR-2 / INV-2: one intent produces one response.
pub struct IdempotentSameResponse;
impl Oracle for IdempotentSameResponse {
    fn id(&self) -> &'static str {
        "IdempotentSameResponse"
    }
    fn covers(&self) -> &'static [&'static str] {
        &["CR-2", "INV-2"]
    }
    fn needs(&self) -> &'static str {
        "creations carrying an idempotency key"
    }
    fn saw_relevant_data(&self, trace: &Trace) -> bool {
        count(trace, |e| matches!(e, TraceEvent::ResponseCreated { key, .. } if !key.is_empty())) > 0
    }
    fn judge(&self, trace: &Trace) -> Verdict {
        let mut by_key: HashMap<String, HashSet<String>> = HashMap::new();
        for e in &trace.events {
            if let TraceEvent::ResponseCreated {
                response_id,
                key,
                ..
            } = e
            {
                if key.is_empty() {
                    continue;
                }
                by_key
                    .entry(key.clone())
                    .or_default()
                    .insert(response_id.clone());
            }
        }
        for (key, ids) in by_key {
            if ids.len() > 1 {
                return Verdict::Fail {
                    evidence: format!("idempotency key {key} produced {} responses: {ids:?}", ids.len()),
                };
            }
        }
        Verdict::Pass
    }
}

/// CR-9 / INV-41 / INV-42: chain resolution stays inside its bounds.
pub struct ChainBounded;
impl Oracle for ChainBounded {
    fn id(&self) -> &'static str {
        "ChainBounded"
    }
    fn covers(&self) -> &'static [&'static str] {
        &["CR-9", "INV-41", "INV-42"]
    }
    fn needs(&self) -> &'static str {
        "chain resolutions or rejections"
    }
    fn saw_relevant_data(&self, trace: &Trace) -> bool {
        count(trace, |e| matches!(e, TraceEvent::ChainResolved { .. } | TraceEvent::ChainRejected { .. })) > 0
    }
    fn judge(&self, trace: &Trace) -> Verdict {
        // Bounds come from `parameters.md` §4.5; a successful resolution must
        // never exceed them, because exceeding them is defined to be an error.
        const MAX_DEPTH: usize = 50;
        const MAX_BYTES: usize = 1024 * 1024;
        for e in &trace.events {
            if let TraceEvent::ChainResolved {
                response_id,
                depth,
                bytes,
                ..
            } = e
            {
                if *depth > MAX_DEPTH {
                    return Verdict::Fail {
                        evidence: format!(
                            "response {response_id}: resolved depth {depth} exceeds {MAX_DEPTH}"
                        ),
                    };
                }
                if *bytes > MAX_BYTES {
                    return Verdict::Fail {
                        evidence: format!(
                            "response {response_id}: resolved {bytes} bytes exceeds {MAX_BYTES}"
                        ),
                    };
                }
            }
        }
        Verdict::Pass
    }
}

/// CR-10 / INV-43 / INV-46: content is never lost quietly.
pub struct NoSilentContentLoss;
impl Oracle for NoSilentContentLoss {
    fn id(&self) -> &'static str {
        "NoSilentContentLoss"
    }
    fn covers(&self) -> &'static [&'static str] {
        &["CR-10", "INV-43", "INV-46"]
    }
    fn needs(&self) -> &'static str {
        "creations requesting storage"
    }
    fn saw_relevant_data(&self, trace: &Trace) -> bool {
        count(trace, |e| matches!(e, TraceEvent::ResponseCreated { store: true, .. })) > 0
    }
    fn judge(&self, trace: &Trace) -> Verdict {
        // A response that asked to be stored must either record that it was, or
        // there must be an explicit rejection in the trace. Anything else means
        // the write vanished while the caller was told it succeeded.
        let mut wanted_storage: HashSet<String> = HashSet::new();
        let mut confirmed: HashSet<String> = HashSet::new();
        let mut rejected = false;

        for e in &trace.events {
            match e {
                TraceEvent::ResponseCreated {
                    response_id,
                    store,
                    outcome,
                    ..
                } if *store && outcome == "accepted" => {
                    wanted_storage.insert(response_id.clone());
                }
                TraceEvent::ContentStored {
                    response_id,
                    stored: true,
                    ..
                } => {
                    confirmed.insert(response_id.clone());
                }
                TraceEvent::ChainRejected { .. } | TraceEvent::CapacityRejected { .. } => {
                    rejected = true;
                }
                _ => {}
            }
        }

        let missing: Vec<_> = wanted_storage.difference(&confirmed).cloned().collect();
        if missing.is_empty() || rejected {
            Verdict::Pass
        } else {
            Verdict::Fail {
                evidence: format!(
                    "responses accepted with store=true but never confirmed stored, and no \
                     explicit rejection was recorded: {missing:?}"
                ),
            }
        }
    }
}

/// CR-13: integrity checks that ran must have passed.
pub struct IntegrityVerified;
impl Oracle for IntegrityVerified {
    fn id(&self) -> &'static str {
        "IntegrityVerified"
    }
    fn covers(&self) -> &'static [&'static str] {
        &["CR-13", "INV-44"]
    }
    fn needs(&self) -> &'static str {
        "integrity checks"
    }
    fn saw_relevant_data(&self, trace: &Trace) -> bool {
        count(trace, |e| matches!(e, TraceEvent::IntegrityChecked { .. })) > 0
    }
    fn judge(&self, trace: &Trace) -> Verdict {
        for e in &trace.events {
            if let TraceEvent::IntegrityChecked {
                response_id,
                ok: false,
                ..
            } = e
            {
                return Verdict::Fail {
                    evidence: format!("response {response_id} failed its integrity check"),
                };
            }
        }
        Verdict::Pass
    }
}

/// CR-11 / INV-51: an aborted attempt still books what it consumed.
pub struct UsageAccounted;
impl Oracle for UsageAccounted {
    fn id(&self) -> &'static str {
        "UsageAccounted"
    }
    fn covers(&self) -> &'static [&'static str] {
        &["CR-11", "INV-51"]
    }
    fn needs(&self) -> &'static str {
        "terminal transitions or partial-usage entries"
    }
    fn saw_relevant_data(&self, trace: &Trace) -> bool {
        count(trace, |e| matches!(e, TraceEvent::PartialUsageRecorded { .. } | TraceEvent::ResponseTerminal { .. })) > 0
    }
    fn judge(&self, trace: &Trace) -> Verdict {
        for e in &trace.events {
            if let TraceEvent::PartialUsageRecorded {
                response_id,
                total_tokens,
                ..
            } = e
            {
                if *total_tokens == 0 {
                    return Verdict::Fail {
                        evidence: format!(
                            "response {response_id} recorded a partial-usage entry of zero tokens, \
                             which cannot be right for a consumed attempt"
                        ),
                    };
                }
            }
        }
        Verdict::Pass
    }
}

/// CR-12 / INV-47: output types are a subset of accepted input types.
pub struct ChainClosure;
impl Oracle for ChainClosure {
    fn id(&self) -> &'static str {
        "ChainClosure"
    }
    fn covers(&self) -> &'static [&'static str] {
        &["CR-12", "INV-47"]
    }
    fn needs(&self) -> &'static str {
        "nothing (structural assertion)"
    }
    fn saw_relevant_data(&self, _trace: &Trace) -> bool {
        // Deliberately always applicable: this oracle asserts a property of the
        // type system, not of the run. Deriving it from the trace would let it
        // pass vacuously whenever a scenario happened to emit no items.
        true
    }
    fn judge(&self, _trace: &Trace) -> Verdict {
        // Checked structurally rather than from the trace: the property is about
        // the type system, so asserting it directly is both stronger and cannot
        // pass vacuously when a scenario happens not to emit items.
        conformance::assert_output_items_are_valid_input();
        Verdict::Pass
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace::Trace;

    fn appended(response_id: &str, sequence_number: u64) -> TraceEvent {
        TraceEvent::EventAppended {
            response_id: response_id.into(),
            attempt: None,
            sequence_number,
            kind: "response.output_text.delta".into(),
            at_ms: 0,
        }
    }

    #[test]
    fn every_advertised_oracle_resolves() {
        for id in all_ids() {
            assert!(builtin(id).is_some(), "{id} is advertised but not built");
        }
        assert!(builtin("NoSuchOracle").is_none());
    }

    #[test]
    fn contiguity_rejects_a_skipped_sequence() {
        let mut t = Trace::new("t");
        t.push(appended("r1", 0));
        t.push(appended("r1", 2));
        assert!(matches!(
            SequenceContiguous.judge(&t),
            Verdict::Fail { .. }
        ));
    }

    #[test]
    fn contiguity_requires_zero_base() {
        let mut t = Trace::new("t");
        t.push(appended("r1", 1));
        assert!(
            matches!(SequenceContiguous.judge(&t), Verdict::Fail { .. }),
            "starting at 1 must fail: it would break starting_after=0"
        );
    }

    #[test]
    fn contiguity_is_per_response() {
        let mut t = Trace::new("t");
        t.push(appended("r1", 0));
        t.push(appended("r2", 0));
        t.push(appended("r1", 1));
        assert_eq!(SequenceContiguous.judge(&t), Verdict::Pass);
    }

    #[test]
    fn expiry_with_data_is_a_contradiction() {
        let mut t = Trace::new("t");
        t.push(TraceEvent::EventRead {
            response_id: "r1".into(),
            starting_after: Some(0),
            count: 3,
            expired: true,
            at_ms: 0,
        });
        assert!(matches!(ExpiredIsExplicit.judge(&t), Verdict::Fail { .. }));
    }

    #[test]
    fn accepted_response_must_reach_a_terminal_state() {
        let mut t = Trace::new("t");
        t.push(TraceEvent::ResponseCreated {
            response_id: "r1".into(),
            key: "k".into(),
            outcome: "accepted".into(),
            store: true,
            previous: None,
            at_ms: 0,
        });
        assert!(matches!(
            CreatedResponsesTerminal.judge(&t),
            Verdict::Fail { .. }
        ));
        t.push(TraceEvent::ResponseTerminal {
            response_id: "r1".into(),
            status: "completed".into(),
            at_ms: 1,
        });
        assert_eq!(CreatedResponsesTerminal.judge(&t), Verdict::Pass);
    }

    #[test]
    fn one_key_may_not_yield_two_responses() {
        let mut t = Trace::new("t");
        for id in ["r1", "r2"] {
            t.push(TraceEvent::ResponseCreated {
                response_id: id.into(),
                key: "same".into(),
                outcome: "accepted".into(),
                store: true,
                previous: None,
                at_ms: 0,
            });
        }
        assert!(matches!(
            IdempotentSameResponse.judge(&t),
            Verdict::Fail { .. }
        ));
    }

    #[test]
    fn stale_attempt_append_is_caught() {
        let mut t = Trace::new("t");
        t.push(TraceEvent::ResponseClaimed {
            response_id: "r1".into(),
            agent_id: uuid::Uuid::new_v4(),
            attempt: 2,
            at_ms: 0,
        });
        t.push(TraceEvent::EventAppended {
            response_id: "r1".into(),
            attempt: Some(1),
            sequence_number: 0,
            kind: "response.output_text.delta".into(),
            at_ms: 1,
        });
        assert!(matches!(StaleAppendRejected.judge(&t), Verdict::Fail { .. }));
    }

    #[test]
    fn unstored_acceptance_without_rejection_is_a_failure() {
        let mut t = Trace::new("t");
        t.push(TraceEvent::ResponseCreated {
            response_id: "r1".into(),
            key: "k".into(),
            outcome: "accepted".into(),
            store: true,
            previous: None,
            at_ms: 0,
        });
        assert!(matches!(
            NoSilentContentLoss.judge(&t),
            Verdict::Fail { .. }
        ));
        t.push(TraceEvent::ContentStored {
            response_id: "r1".into(),
            stored: true,
            at_ms: 1,
        });
        assert_eq!(NoSilentContentLoss.judge(&t), Verdict::Pass);
    }

    #[test]
    fn chain_bounds_are_enforced_on_success_paths() {
        let mut t = Trace::new("t");
        t.push(TraceEvent::ChainResolved {
            response_id: "r1".into(),
            depth: 51,
            items: 10,
            bytes: 10,
            at_ms: 0,
        });
        assert!(matches!(ChainBounded.judge(&t), Verdict::Fail { .. }));
    }

    #[test]
    fn zero_token_partial_usage_is_suspicious() {
        let mut t = Trace::new("t");
        t.push(TraceEvent::PartialUsageRecorded {
            response_id: "r1".into(),
            attempt: 1,
            total_tokens: 0,
            at_ms: 0,
        });
        assert!(matches!(UsageAccounted.judge(&t), Verdict::Fail { .. }));
    }

    #[test]
    fn chain_closure_holds_structurally() {
        assert_eq!(ChainClosure.judge(&Trace::new("t")), Verdict::Pass);
    }
}

#[cfg(test)]
mod vacuity_tests {
    use super::*;
    use crate::trace::Trace;

    fn empty_trace() -> Trace {
        Trace::new("meta-test")
    }

    #[test]
    fn every_builtin_id_resolves() {
        // `all_ids` feeds documentation and tooling; a name listed there but not
        // constructible would be a check nobody could enable.
        for id in all_ids() {
            assert!(builtin(id).is_some(), "{id} is listed but not constructible");
        }
    }

    #[test]
    fn every_oracle_is_reachable_from_all_ids() {
        // The reverse direction: an oracle that exists but is absent from
        // `all_ids` is dead code that looks like coverage.
        assert_eq!(all_ids().len(), 11, "update all_ids when adding an oracle");
    }

    #[test]
    fn unknown_oracle_name_is_an_error() {
        // A typo must not silently disable a check.
        let err = run_oracles(&empty_trace(), &["SequenceContigous".to_string()])
            .expect_err("a misspelled oracle must fail loudly");
        assert!(err.to_string().contains("unknown oracle"));
    }

    #[test]
    fn declaring_an_oracle_with_nothing_to_judge_is_an_error() {
        // The property this whole mechanism exists for. `IntegrityVerified` only
        // fails on a recorded failed check, so on an empty trace its `judge`
        // returns Pass — yet the scenario would have claimed CR-13.
        let trace = empty_trace();
        assert_eq!(IntegrityVerified.judge(&trace), Verdict::Pass);

        let err = run_oracles(&trace, &["IntegrityVerified".to_string()])
            .expect_err("a vacuous oracle must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("no integrity checks to judge"), "{msg}");
        assert!(msg.contains("coverage gate"), "the message must explain why: {msg}");
    }

    #[test]
    fn scan_style_oracles_all_pass_vacuously_on_an_empty_trace() {
        // Documents the hazard the relevance check compensates for: most oracles
        // are written as "scan for a violation, else pass", which is indeed a Pass
        // on an empty trace. This test fails if someone later makes one of them
        // fail-closed, which would make the relevance check redundant for it.
        let trace = empty_trace();
        for id in [
            "SequenceContiguous",
            "SingleClaimPerAttempt",
            "CreatedResponsesTerminal",
            "ExpiredIsExplicit",
            "StaleAppendRejected",
            "IdempotentSameResponse",
            "ChainBounded",
            "NoSilentContentLoss",
            "IntegrityVerified",
            "UsageAccounted",
        ] {
            let o = builtin(id).expect("built in");
            assert_eq!(
                o.judge(&trace),
                Verdict::Pass,
                "{id} unexpectedly fails on an empty trace"
            );
            assert!(
                !o.saw_relevant_data(&trace),
                "{id} claims an empty trace contains data it can judge"
            );
        }
    }

    #[test]
    fn chain_closure_is_applicable_without_trace_data() {
        // Its property is structural, so making it depend on trace contents would
        // reintroduce vacuous passing.
        assert!(ChainClosure.saw_relevant_data(&empty_trace()));
        assert!(run_oracles(&empty_trace(), &["ChainClosure".to_string()]).is_ok());
    }

    #[test]
    fn every_oracle_substantiates_at_least_one_requirement() {
        // An oracle covering nothing cannot affect the coverage gate, so a
        // scenario declaring it gains nothing while appearing to check something.
        for id in all_ids() {
            let o = builtin(id).expect("built in");
            assert!(!o.covers().is_empty(), "{id} covers no requirement");
            assert!(!o.needs().is_empty(), "{id} does not say what it needs");
        }
    }
}
