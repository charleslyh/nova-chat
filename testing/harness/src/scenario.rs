//! L1 YAML scenarios: steps (A/B) + Trace + Oracles (C).

use std::path::Path;

use anyhow::{bail, Context, Result};
use adapters_mem::MemWorld;
use nova_sessions_core::{
    AgentId, Attempt, EventKind, IdempotencyKey, MetaStore, SessionLock, SessionSnapshot,
    SnapshotStore, StreamChannel, StreamError, StreamEvent, SubmitOutcome, TurnStatus,
};
use serde::Deserialize;

use crate::oracle::run_oracles;
use crate::trace::{Trace, TraceEvent};

#[derive(Debug, Deserialize)]
struct ScenarioFile {
    name: String,
    #[serde(default)]
    covers: Vec<String>,
    #[serde(default)]
    oracles: Vec<String>,
    #[serde(default = "default_true")]
    trace: bool,
    #[serde(default)]
    steps: Vec<Step>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum Step {
    CreateSession,
    SubmitTurn {
        text: String,
        key: String,
        /// accepted | duplicate | busy
        #[serde(default)]
        expect: Option<String>,
        /// if false, skip turn_begin append (default: append only on accepted)
        #[serde(default)]
        append_begin: Option<bool>,
    },
    Claim {
        #[serde(default)]
        deadline_ms: Option<u64>,
        /// some | none (default some)
        #[serde(default)]
        expect: Option<String>,
    },
    ClaimAndComplete {
        tokens: Option<usize>,
    },
    AppendDelta {
        payload: Option<String>,
        #[serde(default)]
        expect_stale: bool,
        /// if set, use this attempt instead of last claimed
        attempt: Option<u64>,
    },
    Reap,
    TrimEarliest {
        new_earliest: u64,
    },
    ResumeFrom {
        from_seq: u64,
        expect_min_events: usize,
    },
    ExpectNoGap {
        from_seq: u64,
    },
    ExpectGap {
        from_seq: u64,
    },
    ExpectLock {
        state: String,
    },
    Complete {
        status: Option<String>,
    },
    AdvanceMs {
        by: u64,
    },
    SnapshotPut {
        snapshot_seq: u64,
        /// ok | stale
        #[serde(default)]
        expect: Option<String>,
    },
    SnapshotGet {
        #[serde(default)]
        expect_seq: Option<u64>,
        #[serde(default)]
        expect_none: bool,
    },
    /// INV-32: toggle MemWorld read-only degrade.
    SetReadOnly {
        enabled: bool,
    },
    /// FR-18: set global Pending+Claimed limit.
    SetPendingLimit {
        limit: usize,
    },
}

pub async fn run_l1_dir(dir: &Path) -> Result<Vec<String>> {
    let mut names = Vec::new();
    if !dir.exists() {
        return Ok(names);
    }
    let mut paths: Vec<_> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("yaml"))
        .collect();
    paths.sort();
    for p in paths {
        let name = run_one(&p)
            .await
            .with_context(|| format!("scenario {}", p.display()))?;
        names.push(name);
    }
    Ok(names)
}

async fn run_one(path: &Path) -> Result<String> {
    let text = std::fs::read_to_string(path)?;
    let sc: ScenarioFile = serde_yaml::from_str(&text)?;
    let name = sc.name.clone();
    eprint!("  {name} ... ");
    let mut trace = if sc.trace {
        Trace::with_jsonl_file(&sc.name, Path::new("testing/reports/traces"))?
    } else {
        Trace::new(&sc.name)
    };

    let world = MemWorld::new();
    let mut session = None;
    let mut last_turn = None;
    let mut last_attempt: Option<Attempt> = None;
    let mut now_ms = 1_000u64;

    for step in sc.steps {
        match step {
            Step::CreateSession => {
                let id = world.meta.create_session().await?;
                trace.push(TraceEvent::SessionCreated {
                    session_id: id.0,
                    at_ms: now_ms,
                });
                session = Some(id);
            }
            Step::SubmitTurn {
                text,
                key,
                expect,
                append_begin,
            } => {
                let sid = session.expect("session");
                let out = world
                    .meta
                    .submit_turn(sid, text.clone(), IdempotencyKey(key.clone()), now_ms)
                    .await?;
                let (turn_id, outcome) = match &out {
                    SubmitOutcome::Accepted { turn_id } => (*turn_id, "accepted"),
                    SubmitOutcome::Duplicate { turn_id } => (*turn_id, "duplicate"),
                    SubmitOutcome::Busy => (nova_sessions_core::TurnId(uuid::Uuid::nil()), "busy"),
                    SubmitOutcome::ReadOnly => {
                        (nova_sessions_core::TurnId(uuid::Uuid::nil()), "read_only")
                    }
                    SubmitOutcome::Overloaded => {
                        (nova_sessions_core::TurnId(uuid::Uuid::nil()), "overloaded")
                    }
                };
                if let Some(want) = &expect {
                    if want != outcome {
                        bail!("{}: submit expect {want} got {outcome}", sc.name);
                    }
                } else if outcome == "busy" {
                    bail!("{}: unexpected busy", sc.name);
                } else if outcome == "overloaded" {
                    bail!("{}: unexpected overloaded", sc.name);
                } else if outcome == "read_only" {
                    bail!("{}: unexpected read_only", sc.name);
                }
                trace.push(TraceEvent::TurnSubmitted {
                    session_id: sid.0,
                    turn_id: turn_id.0,
                    key,
                    outcome: outcome.into(),
                    at_ms: now_ms,
                });
                let do_append = append_begin.unwrap_or(outcome == "accepted");
                if do_append && outcome == "accepted" {
                    let seq = world
                        .stream
                        .append(StreamEvent {
                            session_id: sid,
                            seq: 0,
                            kind: EventKind::TurnBegin,
                            turn_id: Some(turn_id),
                            attempt: None,
                            payload: text,
                        })
                        .await?;
                    trace.push(TraceEvent::StreamAppended {
                        session_id: sid.0,
                        turn_id: Some(turn_id.0),
                        attempt: None,
                        seq,
                        kind: "turn_begin".into(),
                        at_ms: now_ms,
                    });
                    last_turn = Some(turn_id);
                } else if outcome == "duplicate" || outcome == "accepted" {
                    last_turn = Some(turn_id);
                }
                trace.push(TraceEvent::MockState {
                    component: "meta".into(),
                    detail: format!("lock={:?}", world.meta.lock(sid).await?),
                    at_ms: now_ms,
                });
            }
            Step::Claim {
                deadline_ms,
                expect,
            } => {
                let sid = session.expect("session");
                let agent = AgentId::new();
                let dl = deadline_ms.unwrap_or(60_000);
                let want = expect.as_deref().unwrap_or("some");
                let got = world.meta.claim_turn(agent, now_ms, dl).await?;
                match (want, got) {
                    ("some", Some(c)) => {
                        last_attempt = Some(c.attempt);
                        let turn_id = c.turn.turn_id;
                        last_turn = Some(turn_id);
                        trace.push(TraceEvent::TurnClaimed {
                            session_id: sid.0,
                            turn_id: turn_id.0,
                            agent_id: agent.0,
                            attempt: c.attempt.0,
                            at_ms: now_ms,
                        });
                    }
                    ("none", None) => {
                        trace.push(TraceEvent::MockState {
                            component: "meta".into(),
                            detail: "claim=none".into(),
                            at_ms: now_ms,
                        });
                    }
                    ("some", None) => bail!("{}: expected claim some, got none", sc.name),
                    ("none", Some(_)) => bail!("{}: expected claim none, got some", sc.name),
                    (other, _) => bail!("{}: unknown claim expect {other}", sc.name),
                }
            }
            Step::ClaimAndComplete { tokens } => {
                let sid = session.expect("session");
                let turn_id = last_turn.expect("turn");
                let agent = AgentId::new();
                let c = world
                    .meta
                    .claim_turn(agent, now_ms, 60_000)
                    .await?
                    .expect("claim");
                last_attempt = Some(c.attempt);
                trace.push(TraceEvent::TurnClaimed {
                    session_id: sid.0,
                    turn_id: turn_id.0,
                    agent_id: agent.0,
                    attempt: c.attempt.0,
                    at_ms: now_ms,
                });
                let n = tokens.unwrap_or(3);
                for i in 0..n {
                    let seq = world
                        .stream
                        .append(StreamEvent {
                            session_id: sid,
                            seq: 0,
                            kind: EventKind::TextDelta,
                            turn_id: Some(turn_id),
                            attempt: Some(c.attempt),
                            payload: format!("t{i}"),
                        })
                        .await?;
                    trace.push(TraceEvent::StreamAppended {
                        session_id: sid.0,
                        turn_id: Some(turn_id.0),
                        attempt: Some(c.attempt.0),
                        seq,
                        kind: "text_delta".into(),
                        at_ms: now_ms,
                    });
                }
                let seq = world
                    .stream
                    .append(StreamEvent {
                        session_id: sid,
                        seq: 0,
                        kind: EventKind::TurnDone,
                        turn_id: Some(turn_id),
                        attempt: Some(c.attempt),
                        payload: "done".into(),
                    })
                    .await?;
                trace.push(TraceEvent::StreamAppended {
                    session_id: sid.0,
                    turn_id: Some(turn_id.0),
                    attempt: Some(c.attempt.0),
                    seq,
                    kind: "turn_done".into(),
                    at_ms: now_ms,
                });
                world
                    .meta
                    .complete_turn(turn_id, c.attempt, TurnStatus::Done)
                    .await?;
                trace.push(TraceEvent::TurnTerminal {
                    turn_id: turn_id.0,
                    status: "done".into(),
                    at_ms: now_ms,
                });
            }
            Step::AppendDelta {
                payload,
                expect_stale,
                attempt,
            } => {
                let sid = session.expect("session");
                let turn_id = last_turn.expect("turn");
                let att = match attempt {
                    Some(a) => Attempt(a),
                    None => last_attempt.expect("attempt"),
                };
                let res = world
                    .stream
                    .append(StreamEvent {
                        session_id: sid,
                        seq: 0,
                        kind: EventKind::TextDelta,
                        turn_id: Some(turn_id),
                        attempt: Some(att),
                        payload: payload.unwrap_or_else(|| "x".into()),
                    })
                    .await;
                match res {
                    Ok(seq) => {
                        if expect_stale {
                            bail!("{}: expected stale append", sc.name);
                        }
                        trace.push(TraceEvent::StreamAppended {
                            session_id: sid.0,
                            turn_id: Some(turn_id.0),
                            attempt: Some(att.0),
                            seq,
                            kind: "text_delta".into(),
                            at_ms: now_ms,
                        });
                    }
                    Err(StreamError::StaleAttempt) => {
                        trace.push(TraceEvent::StreamAppendRejected {
                            session_id: sid.0,
                            turn_id: Some(turn_id.0),
                            attempt: Some(att.0),
                            reason: "stale_attempt".into(),
                            at_ms: now_ms,
                        });
                        if !expect_stale {
                            bail!("{}: unexpected stale append", sc.name);
                        }
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Step::Reap => {
                let aborted = world.meta.reap(now_ms, 90_000).await?;
                for (tid, att, sid) in &aborted {
                    trace.push(TraceEvent::FaultInjected {
                        kind: "reap".into(),
                        target: format!("turn={}", tid.0),
                        at_ms: now_ms,
                    });
                    let _ = world
                        .stream
                        .append(StreamEvent {
                            session_id: *sid,
                            seq: 0,
                            kind: EventKind::AttemptAborted,
                            turn_id: Some(*tid),
                            attempt: None,
                            payload: "reaped".into(),
                        })
                        .await;
                    trace.push(TraceEvent::TurnTerminal {
                        turn_id: tid.0,
                        status: "aborted".into(),
                        at_ms: now_ms,
                    });
                    let _ = att;
                }
                if let Some(sid) = session {
                    trace.push(TraceEvent::MockState {
                        component: "meta".into(),
                        detail: format!("lock={:?} reaped={}", world.meta.lock(sid).await?, aborted.len()),
                        at_ms: now_ms,
                    });
                }
            }
            Step::TrimEarliest { new_earliest } => {
                let sid = session.expect("session");
                world.stream.test_trim_earliest(sid, new_earliest);
                trace.push(TraceEvent::StreamTrimmed {
                    session_id: sid.0,
                    new_earliest,
                    at_ms: now_ms,
                });
                trace.push(TraceEvent::FaultInjected {
                    kind: "trim_hot".into(),
                    target: format!("earliest={new_earliest}"),
                    at_ms: now_ms,
                });
            }
            Step::ResumeFrom {
                from_seq,
                expect_min_events,
            } => {
                let sid = session.expect("session");
                let evs = world.stream.read_from(sid, from_seq, 10_000).await?;
                trace.push(TraceEvent::StreamRead {
                    session_id: sid.0,
                    from_seq,
                    count: evs.len(),
                    gap: false,
                    at_ms: now_ms,
                });
                if evs.len() < expect_min_events {
                    bail!(
                        "{}: resume expected >= {expect_min_events} got {}",
                        sc.name,
                        evs.len()
                    );
                }
            }
            Step::ExpectNoGap { from_seq } => {
                let sid = session.expect("session");
                match world.stream.read_from(sid, from_seq, 1).await {
                    Ok(evs) => {
                        trace.push(TraceEvent::StreamRead {
                            session_id: sid.0,
                            from_seq,
                            count: evs.len(),
                            gap: false,
                            at_ms: now_ms,
                        });
                    }
                    Err(StreamError::Gap(_)) => {
                        trace.push(TraceEvent::StreamRead {
                            session_id: sid.0,
                            from_seq,
                            count: 0,
                            gap: true,
                            at_ms: now_ms,
                        });
                        bail!("{}: unexpected gap at from_seq={from_seq}", sc.name);
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Step::ExpectGap { from_seq } => {
                let sid = session.expect("session");
                match world.stream.read_from(sid, from_seq, 1).await {
                    Ok(evs) => {
                        trace.push(TraceEvent::StreamRead {
                            session_id: sid.0,
                            from_seq,
                            count: evs.len(),
                            gap: false,
                            at_ms: now_ms,
                        });
                        bail!(
                            "{}: expected gap at from_seq={from_seq}, got {} events",
                            sc.name,
                            evs.len()
                        );
                    }
                    Err(StreamError::Gap(_)) => {
                        trace.push(TraceEvent::StreamRead {
                            session_id: sid.0,
                            from_seq,
                            count: 0,
                            gap: true,
                            at_ms: now_ms,
                        });
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Step::ExpectLock { state } => {
                let sid = session.expect("session");
                let lock = world.meta.lock(sid).await?;
                let want = match state.as_str() {
                    "idle" => SessionLock::Idle,
                    "busy" => SessionLock::Busy,
                    other => bail!("unknown lock state {other}"),
                };
                trace.push(TraceEvent::MockState {
                    component: "meta".into(),
                    detail: format!("lock={lock:?} expect={want:?}"),
                    at_ms: now_ms,
                });
                if lock != want {
                    bail!("{}: lock want {want:?} got {lock:?}", sc.name);
                }
            }
            Step::Complete { status } => {
                let turn_id = last_turn.expect("turn");
                let att = last_attempt.expect("attempt");
                let to = match status.as_deref().unwrap_or("done") {
                    "done" => TurnStatus::Done,
                    "failed" => TurnStatus::Failed,
                    other => bail!("unknown complete status {other}"),
                };
                world.meta.complete_turn(turn_id, att, to).await?;
                trace.push(TraceEvent::TurnTerminal {
                    turn_id: turn_id.0,
                    status: format!("{to:?}").to_lowercase(),
                    at_ms: now_ms,
                });
            }
            Step::AdvanceMs { by } => {
                now_ms = now_ms.saturating_add(by);
                trace.push(TraceEvent::Clock { now_ms });
                continue; // skip the default +1 clock at end of loop
            }
            Step::SnapshotPut {
                snapshot_seq,
                expect,
            } => {
                let sid = session.expect("session");
                let snap = SessionSnapshot {
                    session_id: sid,
                    snapshot_seq,
                    bubbles: vec![],
                    running: vec![],
                };
                let want = expect.as_deref().unwrap_or("ok");
                let res = world.snapshot.put(snap).await;
                match (want, res) {
                    ("ok", Ok(())) => {
                        trace.push(TraceEvent::MockState {
                            component: "snapshot".into(),
                            detail: format!("put seq={snapshot_seq} ok"),
                            at_ms: now_ms,
                        });
                    }
                    ("stale", Err(_)) => {
                        trace.push(TraceEvent::MockState {
                            component: "snapshot".into(),
                            detail: format!("put seq={snapshot_seq} stale"),
                            at_ms: now_ms,
                        });
                    }
                    ("ok", Err(e)) => bail!("{}: snapshot put expected ok: {e}", sc.name),
                    ("stale", Ok(())) => {
                        bail!("{}: snapshot put expected stale", sc.name)
                    }
                    (other, _) => bail!("{}: unknown snapshot expect {other}", sc.name),
                }
            }
            Step::SnapshotGet {
                expect_seq,
                expect_none,
            } => {
                let sid = session.expect("session");
                let got = world.snapshot.get(sid).await?;
                if expect_none {
                    if got.is_some() {
                        bail!("{}: snapshot expected none", sc.name);
                    }
                } else if let Some(seq) = expect_seq {
                    let Some(s) = got else {
                        bail!("{}: snapshot expected seq={seq}, got none", sc.name);
                    };
                    if s.snapshot_seq != seq {
                        bail!(
                            "{}: snapshot_seq want {seq} got {}",
                            sc.name,
                            s.snapshot_seq
                        );
                    }
                }
                trace.push(TraceEvent::MockState {
                    component: "snapshot".into(),
                    detail: format!("get expect_seq={expect_seq:?} none={expect_none}"),
                    at_ms: now_ms,
                });
            }
            Step::SetReadOnly { enabled } => {
                world.meta.set_read_only(enabled);
                trace.push(TraceEvent::MockState {
                    component: "meta".into(),
                    detail: format!("read_only={enabled}"),
                    at_ms: now_ms,
                });
            }
            Step::SetPendingLimit { limit } => {
                world.meta.set_pending_limit(limit);
                trace.push(TraceEvent::MockState {
                    component: "meta".into(),
                    detail: format!("pending_limit={limit}"),
                    at_ms: now_ms,
                });
            }
        }
        now_ms += 1;
        trace.push(TraceEvent::Clock { now_ms });
    }

    let _ = sc.covers;
    run_oracles(&trace, &sc.oracles)?;
    eprintln!("ok");
    Ok(name)
}
