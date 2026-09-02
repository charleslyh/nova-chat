//! L1 YAML scenarios: drive the ports directly, record a Trace, judge with Oracles.
//!
//! L1 deliberately bypasses HTTP so a failure localises to the domain layer.
//! The HTTP contract is covered at L2.

use std::collections::HashMap;
use std::path::Path;

use adapters_mem::MemWorld;
use anyhow::{bail, Context, Result};
use nova_responses_core::protocol::{CreateResponseRequest, InputLimits};
use nova_responses_core::{
    canonical_items, AgentId, Attempt, ChainLimits, ContextError, ContextStore, CreateOutcome,
    EventLogError, IdempotencyKey, NodeTag, ResponseEvent, ResponseEventKind, ResponseEventLog,
    ResponseId, ResponseItem, ResponseLedger, ResponseStatus, StoredResponse, TenantId, Usage,
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
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum Step {
    CreateResponse {
        input: String,
        key: String,
        #[serde(default = "default_true")]
        store: bool,
        /// `last` refers to the previously created response; otherwise a label.
        #[serde(default)]
        previous: Option<String>,
        #[serde(default)]
        tenant: Option<String>,
        #[serde(default)]
        instructions: Option<String>,
        /// Label for later reference.
        #[serde(default)]
        label: Option<String>,
        /// accepted | duplicate | overloaded | read_only
        #[serde(default)]
        expect: Option<String>,
    },
    Claim {
        /// some | none
        #[serde(default)]
        expect: Option<String>,
    },
    AppendDelta {
        #[serde(default)]
        payload: Option<String>,
        #[serde(default)]
        attempt: Option<u64>,
        #[serde(default)]
        expect_stale: bool,
    },
    Complete {
        #[serde(default = "default_true")]
        ok: bool,
        #[serde(default)]
        output_text: Option<String>,
        #[serde(default)]
        input_tokens: u64,
        #[serde(default)]
        output_tokens: u64,
    },
    Cancel {
        #[serde(default)]
        tenant: Option<String>,
        /// ok | not_found | invalid_transition
        #[serde(default)]
        expect: Option<String>,
    },
    Reap,
    ReclaimOrphans {
        #[serde(default)]
        expect_min: usize,
    },
    ResumeStartingAfter {
        #[serde(default)]
        starting_after: Option<u64>,
        #[serde(default)]
        expect_min_events: usize,
        #[serde(default)]
        expect_first_sequence: Option<u64>,
    },
    ExpectExpired {
        #[serde(default)]
        starting_after: Option<u64>,
    },
    CloseLog {
        retain_ms: u64,
    },
    SweepEventLogs {
        now_ms: u64,
    },
    ResolveChain {
        #[serde(default)]
        from: Option<String>,
        #[serde(default)]
        tenant: Option<String>,
        #[serde(default)]
        expect_depth: Option<usize>,
        #[serde(default)]
        expect_items: Option<usize>,
        #[serde(default)]
        expect_absent_text: Option<String>,
        #[serde(default)]
        max_depth: Option<usize>,
        #[serde(default)]
        max_bytes: Option<usize>,
    },
    ExpectChainError {
        #[serde(default)]
        from: Option<String>,
        #[serde(default)]
        tenant: Option<String>,
        /// chain_broken | not_stored | cross_tenant | chain_too_long | chain_too_large | unavailable
        reason: String,
        #[serde(default)]
        max_depth: Option<usize>,
        #[serde(default)]
        max_bytes: Option<usize>,
    },
    /// Whether a response's stored content is still retrievable.
    ///
    /// Exists to pin the **blast radius** of a deletion. Without it the suite only
    /// verified that a chain breaks, never what survives — so a change that widened
    /// deletion to cascade downstream, or that made a downstream response
    /// unreadable, would have passed unnoticed.
    ExpectStored {
        #[serde(default)]
        target: Option<String>,
        #[serde(default)]
        tenant: Option<String>,
        /// Expected presence.
        exists: bool,
        /// Optional: item count, to catch content being emptied in place rather
        /// than the record being removed.
        #[serde(default)]
        expect_items: Option<usize>,
    },
    DeleteResponse {
        #[serde(default)]
        target: Option<String>,
        #[serde(default)]
        tenant: Option<String>,
        #[serde(default = "default_true")]
        expect_deleted: bool,
    },
    PurgeTenant {
        #[serde(default)]
        tenant: Option<String>,
        #[serde(default)]
        expect_min: u64,
    },
    SweepExpiredContent {
        now_ms: u64,
        #[serde(default)]
        expect_removed: Option<u64>,
    },
    SetExpiry {
        #[serde(default)]
        target: Option<String>,
        expires_at_ms: u64,
    },
    ExpectIntegrityOk {
        #[serde(default)]
        target: Option<String>,
    },
    TamperContent {
        #[serde(default)]
        target: Option<String>,
    },
    ExpectIntegrityMismatch {
        #[serde(default)]
        target: Option<String>,
    },
    RecordPartialUsage {
        input_tokens: u64,
        output_tokens: u64,
    },
    ExpectPartialUsage {
        min_total_tokens: u64,
    },
    ExpectProtocolReject {
        body: String,
    },
    ExpectProtocolAccept {
        body: String,
    },
    SetReadOnly {
        enabled: bool,
    },
    SetPendingLimit {
        limit: usize,
    },
    SetStoreUnavailable {
        enabled: bool,
    },
    AdvanceMs {
        by: u64,
    },
}

/// Mutable scenario state.
struct Ctx {
    world: MemWorld,
    node_tag: NodeTag,
    now_ms: u64,
    default_tenant: TenantId,
    labels: HashMap<String, ResponseId>,
    last: Option<ResponseId>,
    last_attempt: Option<Attempt>,
}

impl Ctx {
    fn new() -> Self {
        Self {
            world: MemWorld::new(),
            node_tag: NodeTag::parse("node-a").expect("static tag"),
            now_ms: 1_000,
            default_tenant: TenantId::parse("tenant-a").expect("static tenant"),
            labels: HashMap::new(),
            last: None,
            last_attempt: None,
        }
    }

    fn tenant(&self, spec: &Option<String>) -> Result<TenantId> {
        match spec {
            None => Ok(self.default_tenant.clone()),
            Some(raw) => TenantId::parse(raw).map_err(|e| anyhow::anyhow!("tenant `{raw}`: {e}")),
        }
    }

    fn resolve(&self, spec: &Option<String>) -> Result<ResponseId> {
        match spec.as_deref() {
            None | Some("last") => self
                .last
                .clone()
                .ok_or_else(|| anyhow::anyhow!("no response created yet")),
            Some(label) => self
                .labels
                .get(label)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("unknown label `{label}`")),
        }
    }

    fn chain_limits(&self, max_depth: Option<usize>, max_bytes: Option<usize>) -> ChainLimits {
        let base = ChainLimits::default();
        ChainLimits {
            max_depth: max_depth.unwrap_or(base.max_depth),
            max_bytes: max_bytes.unwrap_or(base.max_bytes),
            ..base
        }
    }
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
        if !name.is_empty() {
            names.push(name);
        }
    }
    Ok(names)
}

async fn run_one(path: &Path) -> Result<String> {
    let text = std::fs::read_to_string(path)?;
    if is_blank_scenario(&text) {
        eprintln!("  {} ... skipped (no content)", path.display());
        return Ok(String::new());
    }
    let sc: ScenarioFile = serde_yaml::from_str(&text)?;
    let name = sc.name.clone();
    eprint!("  {name} ... ");

    let mut trace = if sc.trace {
        Trace::with_jsonl_file(&sc.name, Path::new("testing/reports/traces"))?
    } else {
        Trace::new(&sc.name)
    };
    let mut ctx = Ctx::new();

    for step in sc.steps {
        exec(&mut ctx, &mut trace, &sc.name, step).await?;
        ctx.now_ms += 1;
        trace.push(TraceEvent::Clock { now_ms: ctx.now_ms });
    }

    let _ = sc.covers;
    run_oracles(&trace, &sc.oracles)?;
    eprintln!("ok");
    Ok(name)
}

async fn exec(ctx: &mut Ctx, trace: &mut Trace, sc: &str, step: Step) -> Result<()> {
    match step {
        Step::CreateResponse {
            input,
            key,
            store,
            previous,
            tenant,
            instructions,
            label,
            expect,
        } => {
            let tenant_id = ctx.tenant(&tenant)?;
            let previous_id = match &previous {
                None => None,
                Some(spec) => Some(ctx.resolve(&Some(spec.clone()))?),
            };
            // Materialise history exactly as the gateway does (D24): resolve the
            // previous response's full context and snapshot it as a flat copy, so a
            // later deletion of an ancestor cannot strand this response.
            let mut snapshot: Vec<ResponseItem> = Vec::new();
            let mut snapshot_depth: usize = 0;
            if let Some(prev) = &previous_id {
                let resolved = ctx
                    .world
                    .context
                    .resolve_chain(&tenant_id, prev, ChainLimits::default())
                    .await?;
                snapshot = resolved.items;
                snapshot_depth = resolved.depth;
            }
            let id = ResponseId::new(ctx.node_tag.clone());
            let record = StoredResponse {
                response_id: id.clone(),
                previous_response_id: previous_id.clone(),
                tenant_id: tenant_id.clone(),
                model: "test-model".into(),
                instructions: instructions.clone(),
                input_items: vec![ResponseItem::user_text(input)],
                output_items: vec![],
                status: ResponseStatus::Queued,
                usage: Usage::default(),
                created_at_ms: ctx.now_ms,
                completed_at_ms: None,
                stored: store,
                expires_at_ms: None,
                integrity: None,
                integrity_alg: None,
                node_tag: ctx.node_tag.clone(),
                idempotency_key: Some(IdempotencyKey(key.clone())),
                owner: None,
                attempt: Attempt::default(),
                context: snapshot,
                context_depth: snapshot_depth,
            };

            let outcome = ctx
                .world
                .ledger
                .create(record.clone(), IdempotencyKey(key.clone()), ctx.now_ms)
                .await?;
            let (resulting_id, label_str) = match &outcome {
                CreateOutcome::Accepted { response_id } => (response_id.clone(), "accepted"),
                CreateOutcome::Duplicate { response_id } => (response_id.clone(), "duplicate"),
                CreateOutcome::ReadOnly => (id.clone(), "read_only"),
                CreateOutcome::Overloaded => (id.clone(), "overloaded"),
            };
            if let Some(want) = &expect {
                if want != label_str {
                    bail!("{sc}: create expected {want}, got {label_str}");
                }
            } else if label_str != "accepted" {
                bail!("{sc}: unexpected create outcome {label_str}");
            }

            trace.push(TraceEvent::ResponseCreated {
                response_id: resulting_id.to_string(),
                key,
                outcome: label_str.into(),
                store,
                previous: previous_id.as_ref().map(|v| v.to_string()),
                at_ms: ctx.now_ms,
            });

            if label_str == "accepted" {
                if store {
                    ctx.world.context.put(record).await?;
                    trace.push(TraceEvent::ContentStored {
                        response_id: resulting_id.to_string(),
                        stored: true,
                        at_ms: ctx.now_ms,
                    });
                }
                let seq = ctx
                    .world
                    .event_log
                    .append(ResponseEvent {
                        response_id: resulting_id.clone(),
                        sequence_number: 0,
                        kind: ResponseEventKind::Created,
                        attempt: None,
                        payload: String::new(),
                    })
                    .await?;
                trace.push(TraceEvent::EventAppended {
                    response_id: resulting_id.to_string(),
                    attempt: None,
                    sequence_number: seq,
                    kind: ResponseEventKind::Created.as_str().into(),
                    at_ms: ctx.now_ms,
                });
            }

            if let Some(label) = label {
                ctx.labels.insert(label, resulting_id.clone());
            }
            ctx.last = Some(resulting_id);
        }

        Step::Claim { expect } => {
            let agent = AgentId::new();
            let claimed = ctx
                .world
                .ledger
                .claim(&ctx.node_tag, agent, ctx.now_ms, 60_000)
                .await?;
            let want = expect.as_deref().unwrap_or("some");
            match (want, claimed) {
                ("none", None) => {}
                ("none", Some(c)) => bail!("{sc}: expected nothing claimable, got {}", c.record.response_id),
                ("some", None) => bail!("{sc}: expected a claimable response"),
                ("some", Some(c)) => {
                    trace.push(TraceEvent::ResponseClaimed {
                        response_id: c.record.response_id.to_string(),
                        agent_id: agent.0,
                        attempt: c.attempt.0,
                        at_ms: ctx.now_ms,
                    });
                    let seq = ctx
                        .world
                        .event_log
                        .append(ResponseEvent {
                            response_id: c.record.response_id.clone(),
                            sequence_number: 0,
                            kind: ResponseEventKind::InProgress,
                            attempt: Some(c.attempt),
                            payload: String::new(),
                        })
                        .await?;
                    trace.push(TraceEvent::EventAppended {
                        response_id: c.record.response_id.to_string(),
                        attempt: Some(c.attempt.0),
                        sequence_number: seq,
                        kind: ResponseEventKind::InProgress.as_str().into(),
                        at_ms: ctx.now_ms,
                    });
                    ctx.last = Some(c.record.response_id.clone());
                    ctx.last_attempt = Some(c.attempt);
                }
                (other, _) => bail!("{sc}: unknown claim expectation `{other}`"),
            }
        }

        Step::AppendDelta {
            payload,
            attempt,
            expect_stale,
        } => {
            let id = ctx.resolve(&None)?;
            let attempt = attempt
                .map(Attempt)
                .or(ctx.last_attempt)
                .unwrap_or(Attempt(1));
            let result = ctx
                .world
                .event_log
                .append(ResponseEvent {
                    response_id: id.clone(),
                    sequence_number: 0,
                    kind: ResponseEventKind::OutputTextDelta,
                    attempt: Some(attempt),
                    payload: payload.unwrap_or_else(|| "delta".into()),
                })
                .await;
            match (expect_stale, result) {
                (true, Err(EventLogError::StaleAttempt)) => {
                    trace.push(TraceEvent::EventAppendRejected {
                        response_id: id.to_string(),
                        attempt: Some(attempt.0),
                        reason: "stale_attempt".into(),
                        at_ms: ctx.now_ms,
                    });
                }
                (true, other) => bail!("{sc}: expected stale attempt rejection, got {other:?}"),
                (false, Ok(seq)) => {
                    trace.push(TraceEvent::EventAppended {
                        response_id: id.to_string(),
                        attempt: Some(attempt.0),
                        sequence_number: seq,
                        kind: ResponseEventKind::OutputTextDelta.as_str().into(),
                        at_ms: ctx.now_ms,
                    });
                }
                (false, Err(e)) => bail!("{sc}: append failed unexpectedly: {e}"),
            }
        }

        Step::Complete {
            ok,
            output_text,
            input_tokens,
            output_tokens,
        } => {
            let id = ctx.resolve(&None)?;
            let attempt = ctx
                .last_attempt
                .ok_or_else(|| anyhow::anyhow!("{sc}: complete without a claim"))?;
            let status = if ok {
                ResponseStatus::Completed
            } else {
                ResponseStatus::Failed
            };
            let usage = Usage::new(input_tokens, output_tokens);
            let record = ctx
                .world
                .ledger
                .get(&id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("{sc}: response vanished"))?;

            ctx.world
                .ledger
                .complete(&id, attempt, status, usage, ctx.now_ms)
                .await?;

            // Output items are supplied here, not derived from the deltas above
            // (INV-48).
            if record.stored {
                let items = vec![ResponseItem::assistant_text(
                    output_text.clone().unwrap_or_else(|| "answer".into()),
                )];
                ctx.world
                    .context
                    .append_output(&record.tenant_id, &id, items, usage, status, ctx.now_ms)
                    .await?;
            }

            // Server-emitted envelope: no attempt, so the fence cannot reject the
            // very event announcing the transition.
            let seq = ctx
                .world
                .event_log
                .append(ResponseEvent {
                    response_id: id.clone(),
                    sequence_number: 0,
                    kind: if ok {
                        ResponseEventKind::Completed
                    } else {
                        ResponseEventKind::Failed
                    },
                    attempt: None,
                    payload: String::new(),
                })
                .await?;
            trace.push(TraceEvent::EventAppended {
                response_id: id.to_string(),
                attempt: None,
                sequence_number: seq,
                kind: if ok {
                    ResponseEventKind::Completed.as_str().into()
                } else {
                    ResponseEventKind::Failed.as_str().to_string()
                },
                at_ms: ctx.now_ms,
            });
            trace.push(TraceEvent::ResponseTerminal {
                response_id: id.to_string(),
                status: status.as_str().into(),
                at_ms: ctx.now_ms,
            });
        }

        Step::Cancel { tenant, expect } => {
            let id = ctx.resolve(&None)?;
            let tenant_id = ctx.tenant(&tenant)?;
            let result = ctx.world.ledger.cancel(&tenant_id, &id, ctx.now_ms).await;
            let want = expect.as_deref().unwrap_or("ok");
            match (want, result) {
                ("ok", Ok(())) => {
                    trace.push(TraceEvent::ResponseTerminal {
                        response_id: id.to_string(),
                        status: "cancelled".into(),
                        at_ms: ctx.now_ms,
                    });
                }
                ("not_found", Err(nova_responses_core::LedgerError::NotFound)) => {}
                (
                    "invalid_transition",
                    Err(nova_responses_core::LedgerError::InvalidTransition(_)),
                ) => {}
                (want, got) => bail!("{sc}: cancel expected {want}, got {got:?}"),
            }
        }

        Step::Reap => {
            let aborted = ctx.world.ledger.reap(ctx.now_ms, 0).await?;
            for claim in &aborted {
                trace.push(TraceEvent::ResponseTerminal {
                    response_id: claim.response_id.to_string(),
                    status: "failed".into(),
                    at_ms: ctx.now_ms,
                });
            }
            trace.push(TraceEvent::MockState {
                component: "ledger".into(),
                detail: format!("reaped={}", aborted.len()),
                at_ms: ctx.now_ms,
            });
        }

        Step::ReclaimOrphans { expect_min } => {
            let reclaimed = ctx
                .world
                .ledger
                .reclaim_orphans(&ctx.node_tag, ctx.now_ms)
                .await?;
            if reclaimed.len() < expect_min {
                bail!(
                    "{sc}: expected at least {expect_min} orphans reclaimed, got {}",
                    reclaimed.len()
                );
            }
            for claim in &reclaimed {
                trace.push(TraceEvent::ResponseTerminal {
                    response_id: claim.response_id.to_string(),
                    status: "failed".into(),
                    at_ms: ctx.now_ms,
                });
            }
            trace.push(TraceEvent::OrphanReclaimed {
                node_tag: ctx.node_tag.to_string(),
                count: reclaimed.len(),
                at_ms: ctx.now_ms,
            });
        }

        Step::ResumeStartingAfter {
            starting_after,
            expect_min_events,
            expect_first_sequence,
        } => {
            let id = ctx.resolve(&None)?;
            let batch = ctx
                .world
                .event_log
                .read_after(&id, starting_after, 1000, 0)
                .await?;
            trace.push(TraceEvent::EventRead {
                response_id: id.to_string(),
                starting_after,
                count: batch.len(),
                expired: false,
                at_ms: ctx.now_ms,
            });
            if batch.len() < expect_min_events {
                bail!(
                    "{sc}: expected at least {expect_min_events} events after {starting_after:?}, got {}",
                    batch.len()
                );
            }
            if let Some(want) = expect_first_sequence {
                let got = batch.first().map(|e| e.sequence_number);
                if got != Some(want) {
                    bail!("{sc}: expected first sequence {want}, got {got:?}");
                }
            }
        }

        Step::ExpectExpired { starting_after } => {
            let id = ctx.resolve(&None)?;
            match ctx
                .world
                .event_log
                .read_after(&id, starting_after, 10, 0)
                .await
            {
                Err(EventLogError::Expired) => {
                    trace.push(TraceEvent::EventRead {
                        response_id: id.to_string(),
                        starting_after,
                        count: 0,
                        expired: true,
                        at_ms: ctx.now_ms,
                    });
                }
                other => bail!("{sc}: expected an expiry error, got {other:?}"),
            }
        }

        Step::CloseLog { retain_ms } => {
            let id = ctx.resolve(&None)?;
            ctx.world
                .event_log
                .close(&id, ctx.now_ms, retain_ms)
                .await?;
        }

        Step::SweepEventLogs { now_ms } => {
            let swept = ctx.world.event_log.sweep_expired(now_ms).await?;
            trace.push(TraceEvent::MockState {
                component: "event_log".into(),
                detail: format!("swept={swept}"),
                at_ms: ctx.now_ms,
            });
        }

        Step::ResolveChain {
            from,
            tenant,
            expect_depth,
            expect_items,
            expect_absent_text,
            max_depth,
            max_bytes,
        } => {
            let id = ctx.resolve(&from)?;
            let tenant_id = ctx.tenant(&tenant)?;
            let limits = ctx.chain_limits(max_depth, max_bytes);
            let resolved = ctx
                .world
                .context
                .resolve_chain(&tenant_id, &id, limits)
                .await
                .map_err(|e| anyhow::anyhow!("{sc}: chain resolution failed: {e}"))?;
            trace.push(TraceEvent::ChainResolved {
                response_id: id.to_string(),
                depth: resolved.depth,
                items: resolved.items.len(),
                bytes: resolved.bytes,
                at_ms: ctx.now_ms,
            });
            if let Some(want) = expect_depth {
                if resolved.depth != want {
                    bail!("{sc}: expected chain depth {want}, got {}", resolved.depth);
                }
            }
            if let Some(want) = expect_items {
                if resolved.items.len() != want {
                    bail!(
                        "{sc}: expected {want} chain items, got {}",
                        resolved.items.len()
                    );
                }
            }
            if let Some(absent) = expect_absent_text {
                let encoded = canonical_items(&resolved.items);
                if encoded.contains(&absent) {
                    bail!("{sc}: `{absent}` must not appear in chain output: {encoded}");
                }
            }
        }

        Step::ExpectChainError {
            from,
            tenant,
            reason,
            max_depth,
            max_bytes,
        } => {
            let id = ctx.resolve(&from)?;
            let tenant_id = ctx.tenant(&tenant)?;
            let limits = ctx.chain_limits(max_depth, max_bytes);
            let result = ctx
                .world
                .context
                .resolve_chain(&tenant_id, &id, limits)
                .await;
            let matched = match (&reason[..], &result) {
                ("chain_broken", Err(ContextError::ChainBroken(_))) => true,
                ("not_stored", Err(ContextError::NotStored)) => true,
                ("cross_tenant", Err(ContextError::CrossTenant)) => true,
                ("chain_too_long", Err(ContextError::ChainTooLong { .. })) => true,
                ("chain_too_large", Err(ContextError::ChainTooLarge { .. })) => true,
                ("unavailable", Err(ContextError::Unavailable)) => true,
                _ => false,
            };
            if !matched {
                bail!("{sc}: expected chain error `{reason}`, got {result:?}");
            }
            trace.push(TraceEvent::ChainRejected {
                response_id: id.to_string(),
                reason,
                at_ms: ctx.now_ms,
            });
        }

        Step::ExpectStored {
            target,
            tenant,
            exists,
            expect_items,
        } => {
            let id = ctx.resolve(&target)?;
            let tenant_id = ctx.tenant(&tenant)?;
            let found = ctx.world.context.get(&tenant_id, &id).await?;
            match (exists, &found) {
                (true, None) => bail!(
                    "{sc}: expected {id} to still be stored, but it is gone. A deletion \
                     must remove exactly what was asked for — widening it to neighbouring \
                     links destroys content the caller never asked to delete."
                ),
                (false, Some(_)) => {
                    bail!("{sc}: expected {id} to be absent, but it is still stored")
                }
                _ => {}
            }
            if let (Some(want), Some(record)) = (expect_items, &found) {
                let got = record.input_items.len() + record.output_items.len();
                if got != want {
                    bail!("{sc}: expected {want} stored items on {id}, got {got}");
                }
            }
        }

        Step::DeleteResponse {
            target,
            tenant,
            expect_deleted,
        } => {
            let id = ctx.resolve(&target)?;
            let tenant_id = ctx.tenant(&tenant)?;
            let deleted = ctx.world.context.delete(&tenant_id, &id).await?;
            if deleted != expect_deleted {
                bail!("{sc}: expected deleted={expect_deleted}, got {deleted}");
            }
        }

        Step::PurgeTenant { tenant, expect_min } => {
            let tenant_id = ctx.tenant(&tenant)?;
            let removed = ctx.world.context.delete_by_tenant(&tenant_id).await?;
            if removed < expect_min {
                bail!("{sc}: expected at least {expect_min} purged, got {removed}");
            }
        }

        Step::SweepExpiredContent {
            now_ms,
            expect_removed,
        } => {
            let removed = ctx.world.context.sweep_expired(now_ms, 500).await?;
            if let Some(want) = expect_removed {
                if removed != want {
                    bail!("{sc}: expected {want} records swept, got {removed}");
                }
            }
        }

        Step::SetExpiry {
            target,
            expires_at_ms,
        } => {
            let id = ctx.resolve(&target)?;
            let mut record = ctx
                .world
                .ledger
                .get(&id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("{sc}: unknown response"))?;
            record.expires_at_ms = Some(expires_at_ms);
            ctx.world.context.put(record).await?;
        }

        Step::ExpectIntegrityOk { target } => {
            let id = ctx.resolve(&target)?;
            let tenant_id = ctx.default_tenant.clone();
            let ok = ctx.world.context.get(&tenant_id, &id).await.is_ok();
            trace.push(TraceEvent::IntegrityChecked {
                response_id: id.to_string(),
                ok,
                at_ms: ctx.now_ms,
            });
            if !ok {
                bail!("{sc}: integrity check failed unexpectedly");
            }
        }

        Step::TamperContent { target } => {
            let id = ctx.resolve(&target)?;
            let tampered = ctx
                .world
                .context
                .tamper_for_test(&id, vec![ResponseItem::assistant_text("forged")]);
            if !tampered {
                bail!("{sc}: nothing to tamper with");
            }
            trace.push(TraceEvent::FaultInjected {
                kind: "tamper_content".into(),
                target: id.to_string(),
                at_ms: ctx.now_ms,
            });
        }

        Step::ExpectIntegrityMismatch { target } => {
            let id = ctx.resolve(&target)?;
            let tenant_id = ctx.default_tenant.clone();
            match ctx.world.context.get(&tenant_id, &id).await {
                Err(ContextError::IntegrityMismatch) => {}
                other => bail!("{sc}: expected an integrity mismatch, got {other:?}"),
            }
        }

        Step::RecordPartialUsage {
            input_tokens,
            output_tokens,
        } => {
            let id = ctx.resolve(&None)?;
            let attempt = ctx.last_attempt.unwrap_or(Attempt(1));
            let usage = Usage::new(input_tokens, output_tokens);
            ctx.world
                .ledger
                .record_partial_usage(&id, attempt, usage)
                .await?;
            trace.push(TraceEvent::PartialUsageRecorded {
                response_id: id.to_string(),
                attempt: attempt.0,
                total_tokens: usage.total_tokens,
                at_ms: ctx.now_ms,
            });
        }

        Step::ExpectPartialUsage { min_total_tokens } => {
            let id = ctx.resolve(&None)?;
            let total = ctx.world.ledger.total_usage(&id);
            if total.total_tokens < min_total_tokens {
                bail!(
                    "{sc}: expected at least {min_total_tokens} tokens booked, got {}",
                    total.total_tokens
                );
            }
        }

        Step::ExpectProtocolReject { body } => {
            let parsed: Result<CreateResponseRequest, _> = serde_json::from_str(&body);
            let rejected = match parsed {
                Err(_) => true,
                Ok(req) => req.validate(&InputLimits::default()).is_err(),
            };
            if !rejected {
                bail!("{sc}: payload should have been rejected: {body}");
            }
            trace.push(TraceEvent::ProtocolRejected {
                reason: "outside_subset".into(),
                at_ms: ctx.now_ms,
            });
        }

        Step::ExpectProtocolAccept { body } => {
            let req: CreateResponseRequest = serde_json::from_str(&body)
                .map_err(|e| anyhow::anyhow!("{sc}: supported payload rejected: {e}"))?;
            req.validate(&InputLimits::default())
                .map_err(|e| anyhow::anyhow!("{sc}: supported payload failed validation: {e}"))?;
        }

        Step::SetReadOnly { enabled } => {
            ctx.world.ledger.set_read_only(enabled);
            trace.push(TraceEvent::MockState {
                component: "ledger".into(),
                detail: format!("read_only={enabled}"),
                at_ms: ctx.now_ms,
            });
        }

        Step::SetPendingLimit { limit } => {
            ctx.world.ledger.set_pending_limit(limit);
            trace.push(TraceEvent::MockState {
                component: "ledger".into(),
                detail: format!("pending_limit={limit}"),
                at_ms: ctx.now_ms,
            });
        }

        Step::SetStoreUnavailable { enabled } => {
            ctx.world.store.set_unavailable(enabled);
            trace.push(TraceEvent::FaultInjected {
                kind: "store_unavailable".into(),
                target: format!("{enabled}"),
                at_ms: ctx.now_ms,
            });
        }

        Step::AdvanceMs { by } => {
            ctx.now_ms += by;
            trace.push(TraceEvent::Clock { now_ms: ctx.now_ms });
        }
    }
    Ok(())
}

/// Whether a scenario file carries no actual content.
///
/// Comment-only files exist during migrations as tombstones for scenarios that
/// were withdrawn; treating them as parse errors would block the whole run for
/// no benefit.
fn is_blank_scenario(text: &str) -> bool {
    text.lines()
        .map(str::trim)
        .all(|line| line.is_empty() || line.starts_with('#'))
}
