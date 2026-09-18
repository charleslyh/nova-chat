//! L1 YAML scenarios: drive the ports directly, record a Trace, judge with Oracles.
//!
//! L1 deliberately bypasses HTTP so a failure localises to the domain layer.
//! The HTTP contract is covered at L2.

use std::time::Duration;
use std::collections::HashMap;
use std::path::Path;

use mock_server::MemWorld;
use anyhow::{bail, Context, Result};
use nova_responses::protocol::{
    ContentPart, CreateResponseRequest, ItemStatus, ProtocolLimits, ResponseObject,
};
use nova_responses::{
    canonical_items, AgentId, AppendEvent, Attempt, ContextAnchor, Conversation, ConversationId,
    EventBody, IdempotencyKey, ModelParams, NodeTag, ResponseEventKind, ResponseId,
    ResponseItem, ResponseRecord, ResponseStatus, TenantId, TurnCommit, TurnSpec, Usage,
};
use nova_responses::ports::{
    ConversationError, ConversationRepo, ConversationSnapshots, CreateOutcome, EventLogError, ResponseClaimSource, ResponseEventLog, ResponseIntake, StoreError,
    TurnLock,
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
    /// Create a conversation and label it, for snapshot/chain steps (D30).
    CreateConversation {
        label: String,
        #[serde(default)]
        tenant: Option<String>,
    },
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
        /// Conversation this turn belongs to (a `create_conversation` label),
        /// which anchors the turn so `complete` appends to its snapshot (D30).
        #[serde(default)]
        conversation: Option<String>,
        /// accepted | duplicate
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
        /// Announce an output item (`output_item.added`) before the delta, so the
        /// delta streams into an open item — the shape a mid-stream cancel/reap
        /// leaves behind (INV-61).
        #[serde(default)]
        open_item: bool,
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
        /// Assert the snapshot renders this text — the positive counterpart of
        /// `expect_absent_text`, for archival regressions (INV-61).
        #[serde(default)]
        expect_text: Option<String>,
        #[serde(default)]
        expect_absent_text: Option<String>,
    },
    ExpectChainError {
        #[serde(default)]
        from: Option<String>,
        #[serde(default)]
        tenant: Option<String>,
        /// not_found | unavailable
        reason: String,
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
    /// Conversation labels (D30): chain/context steps read a conversation's
    /// snapshot, so scenarios label conversations separately.
    convs: HashMap<String, ConversationId>,
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
            convs: HashMap::new(),
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

    fn resolve_conv(&self, spec: &Option<String>) -> Result<ConversationId> {
        match spec.as_deref() {
            None | Some("last") => self
                .convs
                .values()
                .next()
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("no conversation created yet")),
            Some(label) => self
                .convs
                .get(label)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("unknown conversation label `{label}`")),
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
        Trace::with_jsonl_file(&sc.name, Path::new("verify/reports/traces"))?
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
        Step::CreateConversation { label, tenant } => {
            let tenant_id = ctx.tenant(&tenant)?;
            let conversation = ctx
                .world
                .conversation
                .create(Conversation::new(
                    ConversationId::new(),
                    tenant_id,
                    Default::default(),
                    ctx.now_ms,
                ))
                .await?;
            ctx.convs.insert(label.clone(), conversation.id.clone());
            trace.push(TraceEvent::MockState {
                component: "conversation".into(),
                detail: format!("created={}", conversation.id),
                at_ms: ctx.now_ms,
            });
        }
        Step::CreateResponse {
            input,
            key,
            store,
            previous,
            tenant,
            instructions,
            label,
            expect,
            conversation,
        } => {
            let tenant_id = ctx.tenant(&tenant)?;
            let previous_id = match &previous {
                None => None,
                Some(spec) => Some(ctx.resolve(&Some(spec.clone()))?),
            };
            let conversation_id = match &conversation {
                None => None,
                Some(spec) => Some(ctx.resolve_conv(&Some(spec.clone()))?),
            };
            // D30: the record holds metadata only — no materialised snapshot. History
            // is read from the conversation snapshot at execution time.
            let id = ResponseId::new(ctx.node_tag.clone());
            // One anchor, so a scenario cannot ask for a conversation *and* a previous
            // response and leave the store to pick.
            let anchor = match (conversation_id, previous_id.clone()) {
                (Some(conversation_id), None) => ContextAnchor::Conversation(conversation_id),
                (None, Some(previous)) => ContextAnchor::Previous(previous),
                (None, None) => ContextAnchor::Root,
                (Some(_), Some(_)) => bail!(
                    "{sc}: a step may name a conversation or a previous response, not both"
                ),
            };
            let idempotency_key = IdempotencyKey::parse(&key)
                .map_err(|e| anyhow::anyhow!("{sc}: idempotency key `{key}`: {e}"))?;
            let record = ResponseRecord::queued(
                id.clone(),
                tenant_id.clone(),
                TurnSpec {
                    params: ModelParams {
                        instructions: instructions.clone(),
                        ..ModelParams::new("test-model")
                    },
                    input_items: vec![ResponseItem::user_text(input)],
                    store,
                    ext: None,
                    anchor,
                },
                idempotency_key.clone(),
                ctx.now_ms,
                0,
            );

            let outcome = ctx
                .world
                .ledger
                .create(record.clone(), idempotency_key, ctx.now_ms)
                .await?;
            let (resulting_id, label_str) = match &outcome {
                CreateOutcome::Accepted(record) => (record.response_id.clone(), "accepted"),
                CreateOutcome::Duplicate(record) => (record.response_id.clone(), "duplicate"),
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

            // D30: a store=true response commits its input at create time, so
            // admission is the point where its content becomes durably stored.
            // Recording it keeps NoSilentContentLoss able to see that an accepted
            // store=true response was not silently dropped.
            if store && label_str == "accepted" {
                trace.push(TraceEvent::ContentStored {
                    response_id: resulting_id.to_string(),
                    stored: true,
                    at_ms: ctx.now_ms,
                });
            }

            if label_str == "accepted" {
                let seq = ctx
                    .world
                    .event_log
                    .append(AppendEvent::lifecycle(
                        resulting_id.clone(),
                        ResponseEventKind::Created,
                        ResponseObject::terminal_stub(&resulting_id, ResponseStatus::Queued),
                    ))
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
                .claim(agent, ctx.now_ms, Duration::from_millis(60_000))
                .await?;
            let want = expect.as_deref().unwrap_or("some");
            match (want, claimed) {
                ("none", None) => {}
                ("none", Some(c)) => bail!("{sc}: expected nothing claimable, got {}", c.record.response_id),
                ("some", None) => bail!("{sc}: expected a claimable response"),
                ("some", Some(c)) => {
                    trace.push(TraceEvent::ResponseClaimed {
                        response_id: c.record.response_id.to_string(),
                        agent_id: agent.uuid(),
                        attempt: c.record.attempt.0,
                        at_ms: ctx.now_ms,
                    });
                    let seq = ctx
                        .world
                        .event_log
                        .append(AppendEvent::lifecycle_with_attempt(
                            c.record.response_id.clone(),
                            ResponseEventKind::InProgress,
                            c.record.attempt,
                            ResponseObject::terminal_stub(&c.record.response_id, ResponseStatus::InProgress),
                        ))
                        .await?;
                    trace.push(TraceEvent::EventAppended {
                        response_id: c.record.response_id.to_string(),
                        attempt: Some(c.record.attempt.0),
                        sequence_number: seq,
                        kind: ResponseEventKind::InProgress.as_str().into(),
                        at_ms: ctx.now_ms,
                    });
                    ctx.last = Some(c.record.response_id.clone());
                    ctx.last_attempt = Some(c.record.attempt);
                }
                (other, _) => bail!("{sc}: unknown claim expectation `{other}`"),
            }
        }

        Step::AppendDelta {
            payload,
            attempt,
            open_item,
            expect_stale,
        } => {
            let id = ctx.resolve(&None)?;
            let attempt = attempt
                .map(Attempt)
                .or(ctx.last_attempt)
                .unwrap_or(Attempt(1));
            if open_item {
                ctx.world
                    .event_log
                    .append(AppendEvent::item(
                        id.clone(),
                        attempt,
                        false,
                        0,
                        ResponseItem::Message {
                            role: nova_responses::Role::Assistant,
                            content: vec![],
                            id: Some("m0".into()),
                            status: Some(ItemStatus::InProgress),
                        },
                    ))
                    .await
                    .map_err(|e| anyhow::anyhow!("{sc}: open item append failed: {e}"))?;
                trace.push(TraceEvent::EventAppended {
                    response_id: id.to_string(),
                    attempt: Some(attempt.0),
                    sequence_number: 0,
                    kind: "response.output_item.added".into(),
                    at_ms: ctx.now_ms,
                });
            }
            let result = ctx
                .world
                .event_log
                .append(AppendEvent::text_delta(
                    id.clone(),
                    attempt,
                    String::new(),
                    0,
                    0,
                    payload.unwrap_or_else(|| "delta".into()),
                ))
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
            let record = ResponseIntake::get(ctx.world.ledger.as_ref(), &id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("{sc}: response vanished"))?;

            ctx.world
                .ledger
                .complete(&id, attempt, status, usage, ctx.now_ms)
                .await?;

            // Output items are supplied here, not derived from the deltas above
            // (INV-48). Under D30 they go to the conversation snapshot when the
            // response is conversation-anchored.
            if record.is_stored() {
                if let Some(conversation_id) = &record.conversation_id() {
                    let items = vec![ResponseItem::assistant_text(
                        output_text.clone().unwrap_or_else(|| "answer".into()),
                    )];
                    ctx.world
                        .conversation
                        .append_turn(
                            &record.tenant_id,
                            conversation_id,
                            &id,
                            TurnCommit {
                                input_items: record.spec.input_items.clone(),
                                output_items: items,
                                reasoning: None,
                                usage,
                                status,
                            },
                            ctx.now_ms,
                        )
                        .await?;
                }
            }

            // Server-emitted envelope: no attempt, so the fence cannot reject the
            // very event announcing the transition.
            let seq = ctx
                .world
                .event_log
                .append(AppendEvent::lifecycle(
                    id.clone(),
                    if ok {
                        ResponseEventKind::Completed
                    } else {
                        ResponseEventKind::Failed
                    },
                    ResponseObject::terminal_stub(&id, status),
                ))
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
            // Terminal, so the session bookkeeping is owed here too. This step
            // stands in for the engine's terminal funnel, and a stand-in that
            // skipped it would let a scenario pass while the real path is broken.
            settle_session(ctx, &record, status).await?;

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
                    if let Some(record) = ResponseIntake::get(ctx.world.ledger.as_ref(), &id).await? {
                        settle_session(ctx, &record, ResponseStatus::Cancelled).await?;
                    }
                    trace.push(TraceEvent::ResponseTerminal {
                        response_id: id.to_string(),
                        status: "cancelled".into(),
                        at_ms: ctx.now_ms,
                    });
                }
                ("not_found", Err(nova_responses::ports::LedgerError::NotFound)) => {}
                (
                    "invalid_transition",
                    Err(nova_responses::ports::LedgerError::InvalidTransition(_)),
                ) => {}
                (want, got) => bail!("{sc}: cancel expected {want}, got {got:?}"),
            }
        }

        Step::Reap => {
            let aborted = ctx.world.ledger.reap(ctx.now_ms, Duration::from_millis(0)).await?;
            for claim in &aborted {
                // Archive the reaped turn's input plus completed output (INV-61) **before**
                // the lock is released, mirroring the sweeper's ordering: a client that
                // acts on `turn_completed` must not read a snapshot still missing this
                // turn's input.
                if claim.store {
                    if let Some(conversation_id) = &claim.conversation_id {
                        archive_incomplete_turn(
                            ctx,
                            &claim.tenant_id,
                            conversation_id,
                            &claim.response_id,
                            &claim.input_items,
                            ResponseStatus::Failed,
                        )
                        .await?;
                    }
                }
                // Reap is the only release a reaped response gets: its holder is
                // gone and the fence has moved, so that holder's own terminal path
                // is refused as stale.
                if let Some(conversation_id) = &claim.conversation_id {
                    ctx.world
                        .conversation
                        .release_active(
                            &claim.tenant_id,
                            conversation_id,
                            &claim.response_id,
                            ResponseStatus::Failed,
                            ctx.now_ms,
                        )
                        .await?;
                }
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

        Step::ResumeStartingAfter {
            starting_after,
            expect_min_events,
            expect_first_sequence,
        } => {
            let id = ctx.resolve(&None)?;
            let batch = ctx
                .world
                .event_log
                .read_after(&id, starting_after, 1000, Duration::from_millis(0))
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
                let got = batch.first().map(|e| e.sequence_number());
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
                .read_after(&id, starting_after, 10, Duration::from_millis(0))
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
                .close(&id, ctx.now_ms, Duration::from_millis(retain_ms))
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
            expect_text,
            expect_absent_text,
        } => {
            let conv = ctx.resolve_conv(&from)?;
            let tenant_id = ctx.tenant(&tenant)?;
            let resolved = ctx
                .world
                .conversation
                .read_snapshot(&tenant_id, &conv)
                .await
                .map_err(|e| anyhow::anyhow!("{sc}: snapshot read failed: {e}"))?;
            trace.push(TraceEvent::ChainResolved {
                response_id: conv.to_string(),
                depth: resolved.turns,
                items: resolved.item_count(),
                bytes: resolved.bytes(),
                at_ms: ctx.now_ms,
            });
            if let Some(want) = expect_depth {
                if resolved.turns != want {
                    bail!("{sc}: expected chain depth {want}, got {}", resolved.turns);
                }
            }
            if let Some(want) = expect_items {
                if resolved.item_count() != want {
                    bail!(
                        "{sc}: expected {want} chain items, got {}",
                        resolved.item_count()
                    );
                }
            }
            if let Some(want) = expect_text {
                let encoded = canonical_items(&resolved.clone().into_items());
                if !encoded.contains(&want) {
                    bail!("{sc}: `{want}` must appear in chain output: {encoded}");
                }
            }
            if let Some(absent) = expect_absent_text {
                let encoded = canonical_items(&resolved.clone().into_items());
                if encoded.contains(&absent) {
                    bail!("{sc}: `{absent}` must not appear in chain output: {encoded}");
                }
            }
        }

        Step::ExpectChainError {
            from,
            tenant,
            reason,
        } => {
            let conv = ctx.resolve_conv(&from)?;
            let tenant_id = ctx.tenant(&tenant)?;
            let result = ctx
                .world
                .conversation
                .read_snapshot(&tenant_id, &conv)
                .await;
            let matched = matches!(
                (&reason[..], &result),
                ("not_found", Err(ConversationError::NotFound))
                    | (
                        "unavailable",
                        Err(ConversationError::Store(StoreError::Unavailable))
                    )
            );
            if !matched {
                bail!("{sc}: expected chain error `{reason}`, got {result:?}");
            }
            trace.push(TraceEvent::ChainRejected {
                response_id: conv.to_string(),
                reason,
                at_ms: ctx.now_ms,
            });
        }

        Step::ExpectStored {
            target,
            exists,
            expect_items,
        } => {
            let id = ctx.resolve(&target)?;
            let found = ResponseIntake::get(ctx.world.ledger.as_ref(), &id).await?;
            match (exists, &found) {
                (true, None) => bail!("{sc}: expected {id} to still be stored, but it is gone"),
                (false, Some(_)) => {
                    bail!("{sc}: expected {id} to be absent, but it is still stored")
                }
                _ => {}
            }
            if let (Some(want), Some(record)) = (expect_items, &found) {
                let got = record.spec.input_items.len();
                if got != want {
                    bail!("{sc}: expected {want} stored items on {id}, got {got}");
                }
            }
        }

        Step::DeleteResponse {
            target,
            expect_deleted,
        } => {
            let id = ctx.resolve(&target)?;
            let deleted = ctx.world.ledger.delete(&id).await?;
            if deleted != expect_deleted {
                bail!("{sc}: expected deleted={expect_deleted}, got {deleted}");
            }
        }

        Step::PurgeTenant { tenant, expect_min } => {
            let tenant_id = ctx.tenant(&tenant)?;
            let removed = ctx.world.ledger.delete_by_tenant(&tenant_id).await?;
            if removed < expect_min {
                bail!("{sc}: expected at least {expect_min} purged, got {removed}");
            }
        }

        Step::SweepExpiredContent {
            expect_removed,
        } => {
            // D30: durable content lives in the conversation snapshot (no per-response
            // expiry to sweep). Scenarios that assert sweep behaviour need rework.
            if let Some(want) = expect_removed {
                if want != 0 {
                    bail!("{sc}: content-expiry sweep no longer exists under D30");
                }
            }
        }

        Step::SetExpiry {
            target,
            expires_at_ms,
        } => {
            let id = ctx.resolve(&target)?;
            let mut record = ResponseIntake::get(ctx.world.ledger.as_ref(), &id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("{sc}: unknown response"))?;
            record.expires_at_ms = Some(expires_at_ms);
            // No context store to update the record back through; expiry now only
            // gates response retrieval via the event stream TTL (D30).
        }

        Step::ExpectIntegrityOk { target } => {
            let id = ctx.resolve(&target)?;
            let ok = ResponseIntake::get(ctx.world.ledger.as_ref(), &id).await.is_ok();
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
                .ledger
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
            // D30: the mock signs input on create but does not re-verify on read;
            // tamper-detection on read is a documented follow-up.
            let _ = id;
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
                total_tokens: usage.total_tokens(),
                at_ms: ctx.now_ms,
            });
        }

        Step::ExpectPartialUsage { min_total_tokens } => {
            let id = ctx.resolve(&None)?;
            let total = ctx.world.ledger.total_usage(&id);
            if total.total_tokens() < min_total_tokens {
                bail!(
                    "{sc}: expected at least {min_total_tokens} tokens booked, got {}",
                    total.total_tokens()
                );
            }
        }

        Step::ExpectProtocolReject { body } => {
            let parsed: Result<CreateResponseRequest, _> = serde_json::from_str(&body);
            let rejected = match parsed {
                Err(_) => true,
                Ok(req) => req.validate(&ProtocolLimits::default()).is_err(),
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
            req.validate(&ProtocolLimits::default())
                .map_err(|e| anyhow::anyhow!("{sc}: supported payload failed validation: {e}"))?;
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

/// Replay a response's event stream for its archivable output items, in
/// `output_index` order — the harness's stand-in for the service layer's archival
/// replay (INV-48 RESTATE, INV-61). Used by the cancel/reap steps, where no engine
/// is running to submit a terminal result. Mirrors production: items that reached
/// `done` come back as they completed, and the trailing item still being streamed
/// is reconstructed from its text deltas as an `ItemStatus::Incomplete` message.
async fn replay_completed_items(
    event_log: &dyn ResponseEventLog,
    response_id: &ResponseId,
) -> Result<Vec<ResponseItem>> {
    let mut items: Vec<(u32, ResponseItem)> = Vec::new();
    let mut open: Option<(u32, ResponseItem)> = None;
    let mut open_text = String::new();
    let mut cursor: Option<u64> = None;
    loop {
        let batch = event_log
            .read_after(response_id, cursor, 256, Duration::ZERO)
            .await
            .context("replaying completed items for archival")?;
        if batch.is_empty() {
            break;
        }
        cursor = batch.last().map(|e| e.sequence_number());
        for event in &batch {
            match event.kind() {
                ResponseEventKind::OutputItemAdded => {
                    if let EventBody::Item { output_index, item } = event.body() {
                        open = Some((*output_index, item.clone()));
                        open_text.clear();
                    }
                }
                ResponseEventKind::OutputItemDone => {
                    if let EventBody::Item { output_index, item } = event.body() {
                        if open.as_ref().is_some_and(|(idx, _)| idx == output_index) {
                            open = None;
                            open_text.clear();
                        }
                        items.push((*output_index, item.clone()));
                    }
                }
                ResponseEventKind::OutputTextDelta => {
                    if let EventBody::Delta {
                        output_index,
                        delta,
                        ..
                    } = event.body()
                    {
                        if open.as_ref().is_some_and(|(idx, _)| idx == output_index) {
                            open_text.push_str(delta);
                        }
                    }
                }
                _ => {}
            }
        }
    }
    if let Some((index, ResponseItem::Message { role, id, .. })) = open {
        if !open_text.is_empty() {
            items.push((
                index,
                ResponseItem::Message {
                    role,
                    content: vec![ContentPart::OutputText { text: open_text }],
                    id,
                    status: Some(ItemStatus::Incomplete),
                },
            ));
        }
    }
    items.sort_by_key(|(index, _)| *index);
    Ok(items.into_iter().map(|(_, item)| item).collect())
}

/// Archive an incomplete turn (failed/cancelled/reaped) to the conversation snapshot:
/// the turn's input plus whatever output reached a `done` boundary. Mirrors the service
/// layer's cancel/reap archival (INV-61) so L1 scenarios exercise the same behaviour
/// production shows. `append_turn`'s per-response idempotency makes this safe even when
/// the engine stand-in already committed the turn: the repeat returns the assigned
/// index and appends nothing.
async fn archive_incomplete_turn(
    ctx: &Ctx,
    tenant: &TenantId,
    conversation_id: &ConversationId,
    response_id: &ResponseId,
    input_items: &[ResponseItem],
    status: ResponseStatus,
) -> Result<()> {
    let output_items = replay_completed_items(ctx.world.event_log.as_ref(), response_id).await?;
    ctx.world
        .conversation
        .append_turn(
            tenant,
            conversation_id,
            response_id,
            TurnCommit {
                input_items: input_items.to_vec(),
                output_items,
                reasoning: None,
                usage: Usage::default(),
                status,
            },
            ctx.now_ms,
        )
        .await
        .context("archiving an incomplete turn to the conversation snapshot")?;
    Ok(())
}

/// Session and conversation bookkeeping for a response that just reached a
/// terminal state.
///
/// These steps stand in for the engine's terminal funnel, so they owe the same
/// two effects. Mirrors `Agent::settle`, including the ordering: the tail is
/// advanced **before** the lock is released, so a client that acts on
/// `turn_completed` cannot read a tail that has not moved yet.
///
/// A record with no association is a no-op, which is the common case for the
/// scenarios that predate sessions.
async fn settle_session(
    ctx: &Ctx,
    record: &ResponseRecord,
    status: ResponseStatus,
) -> anyhow::Result<()> {
    // Only a turn that committed output may advance the tail; a failed or
    // cancelled turn would leave the conversation ending on an unanswered
    // question.
    if matches!(
        status,
        ResponseStatus::Completed | ResponseStatus::Incomplete
    ) {
        if let Some(conversation_id) = &record.conversation_id() {
            ctx.world
                .conversation
                .advance(&record.tenant_id, conversation_id, &record.response_id)
                .await?;
        }
    }
    // An incomplete turn still archives its input and whatever output completed
    // (INV-61), before the lock is released — the same ordering the engine and the
    // service layer use. Idempotent per response, so a turn the engine stand-in
    // already committed is left untouched.
    if matches!(status, ResponseStatus::Failed | ResponseStatus::Cancelled) && record.is_stored() {
        if let Some(conversation_id) = &record.conversation_id() {
            archive_incomplete_turn(
                ctx,
                &record.tenant_id,
                conversation_id,
                &record.response_id,
                &record.spec.input_items,
                status,
            )
            .await?;
        }
    }
    if let Some(conversation_id) = &record.conversation_id() {
        ctx.world
            .conversation
            .release_active(
                &record.tenant_id,
                conversation_id,
                &record.response_id,
                status,
                ctx.now_ms,
            )
            .await?;
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
