use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use nova_adapter_mem::MemWorld;
use nova_claim::ClaimService;
use nova_core::{Priority, TaskKind, TaskState};
use nova_matcher::Matcher;
use nova_ports::{
    CapacityLedger, Clock, IdempotencyGate, Reservation, TaskStore,
};
use serde::Deserialize;

use crate::mock_worker::{sample_task, MockWorker};
use crate::oracles::{select_oracles, Verdict};
use crate::trace::{Trace, TraceEvent};

#[derive(Debug, Deserialize)]
pub struct ScenarioSpec {
    pub id: String,
    #[serde(default)]
    pub topology: String,
    #[serde(default)]
    pub oracles: Vec<String>,
    #[serde(default)]
    pub covers: Vec<String>,
    #[serde(default)]
    pub steps: Vec<Step>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Step {
    Submit {
        kind: String,
        #[serde(default = "default_units")]
        units: u32,
        #[serde(default)]
        key: Option<String>,
    },
    RegisterWorker {
        capacity: u32,
    },
    ClaimAndComplete {
        #[serde(default = "default_true")]
        success: bool,
    },
    AdvanceMs {
        ms: u64,
    },
    ExpectBackpressure,
}

fn default_units() -> u32 {
    2
}
fn default_true() -> bool {
    true
}

fn parse_kind(s: &str) -> TaskKind {
    match s {
        "aigc_image" => TaskKind::AigcImage,
        "aigc_video" => TaskKind::AigcVideo,
        "agent" => TaskKind::Agent,
        _ => TaskKind::Agent,
    }
}

pub async fn run_scenario(spec: &ScenarioSpec) -> Result<Trace> {
    let world = MemWorld::new();
    let matcher = Matcher::new("scenario");
    let claim = ClaimService::new(
        world.store.clone() as Arc<dyn TaskStore>,
        world.ledger.clone() as Arc<dyn CapacityLedger>,
        world.sandbox.clone() as Arc<dyn nova_ports::PolicySandbox>,
        world.clock.clone() as Arc<dyn Clock>,
        matcher,
        10_000,
    );

    let mut trace = Trace::default();
    let mut worker: Option<MockWorker> = None;

    for step in &spec.steps {
        match step {
            Step::RegisterWorker { capacity } => {
                let mw = MockWorker::new(*capacity, TaskKind::Agent);
                world
                    .ledger
                    .register_worker(mw.profile.id, *capacity)
                    .await?;
                worker = Some(mw);
            }
            Step::Submit { kind, units, key } => {
                let now = world.clock.now_ms().await;
                let task = sample_task(parse_kind(kind), Priority::Normal, *units, now);
                let ikey = nova_core::IdempotencyKey(
                    key.clone().unwrap_or_else(|| task.id.0.to_string()),
                );
                match world.gate.reserve(&ikey).await? {
                    Reservation::AlreadyExists => {
                        trace.push(TraceEvent::Rejected {
                            reason: "idempotent".into(),
                            at_ms: now,
                        });
                    }
                    Reservation::Reserved => {
                        world.store.insert(task.clone()).await?;
                        trace.push(TraceEvent::Submitted {
                            task: task.id,
                            key: ikey,
                            at_ms: now,
                        });
                    }
                }
            }
            Step::ClaimAndComplete { success } => {
                let mw = worker.as_ref().context("RegisterWorker first")?;
                let now = world.clock.now_ms().await;
                match claim.claim_one(&mw.profile).await {
                    Ok((task, attempt)) => {
                        trace.push(TraceEvent::Claimed {
                            task: task.id,
                            worker: mw.profile.id,
                            attempt,
                            at_ms: now,
                        });
                        trace.push(TraceEvent::CapacityChanged {
                            worker: mw.profile.id,
                            delta: task.capacity.units as i64,
                            at_ms: now,
                        });
                        mw.emit_progress(world.stream.clone(), &task, attempt)
                            .await;
                        claim
                            .complete(
                                &task.id,
                                &mw.profile.id,
                                attempt,
                                task.capacity.units,
                                *success,
                            )
                            .await?;
                        trace.push(TraceEvent::CapacityChanged {
                            worker: mw.profile.id,
                            delta: -(task.capacity.units as i64),
                            at_ms: now,
                        });
                        let state = if *success { "succeeded" } else { "failed" };
                        trace.push(TraceEvent::StateChanged {
                            task: task.id,
                            from: format!("{:?}", TaskState::Claimed),
                            to: state.into(),
                            at_ms: now,
                        });
                        trace.push(TraceEvent::Terminal {
                            task: task.id,
                            state: state.into(),
                            at_ms: now,
                        });
                    }
                    Err(e) => {
                        trace.push(TraceEvent::ClaimAttempted {
                            task: TaskIdPlaceholder(),
                            worker: mw.profile.id,
                            ok: false,
                        });
                        bail!("claim failed: {e}");
                    }
                }
            }
            Step::AdvanceMs { ms } => {
                world.clock.advance(*ms);
            }
            Step::ExpectBackpressure => {
                // Fill pending to threshold — use a tiny claim service local threshold in dedicated scenario.
                // For default threshold 10000, submit many is expensive; scenarios use custom id check.
                let now = world.clock.now_ms().await;
                // Soft check: if scenario asks, try submit_allowed on a tight service.
                let tight = ClaimService::new(
                    world.store.clone() as Arc<dyn TaskStore>,
                    world.ledger.clone() as Arc<dyn CapacityLedger>,
                    world.sandbox.clone() as Arc<dyn nova_ports::PolicySandbox>,
                    world.clock.clone() as Arc<dyn Clock>,
                    Matcher::new("bp"),
                    1,
                );
                // ensure at least one pending
                let t = sample_task(TaskKind::Agent, Priority::Low, 1, now);
                world.store.insert(t).await?;
                match tight.submit_allowed().await {
                    Err(nova_claim::ClaimError::Backpressure) => {
                        trace.push(TraceEvent::Rejected {
                            reason: "backpressure".into(),
                            at_ms: now,
                        });
                    }
                    other => bail!("expected backpressure, got {other:?}"),
                }
            }
        }
    }

    let oracles = select_oracles(&spec.oracles);
    for o in oracles {
        match o.judge(&trace) {
            Verdict::Pass => {}
            Verdict::Fail { evidence } => {
                bail!("oracle {} failed: {evidence}", o.id());
            }
        }
    }
    let _ = &spec.covers;
    let _ = &spec.topology;
    Ok(trace)
}

/// Dummy for failed claim path — avoid needing a real id.
#[allow(non_snake_case)]
fn TaskIdPlaceholder() -> nova_core::TaskId {
    nova_core::TaskId::new()
}

pub async fn run_scenario_file(path: &Path) -> Result<()> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read {}", path.display()))?;
    let spec: ScenarioSpec = serde_yaml::from_str(&text)?;
    run_scenario(&spec).await?;
    Ok(())
}

pub async fn run_l1_dir(dir: &Path) -> Result<usize> {
    let mut n = 0;
    let mut paths: Vec<_> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension()
                .and_then(|x| x.to_str())
                .is_some_and(|x| x == "yaml" || x == "yml")
        })
        .collect();
    paths.sort();
    for p in paths {
        tracing::info!("L1 scenario {}", p.display());
        run_scenario_file(&p).await?;
        n += 1;
    }
    Ok(n)
}
