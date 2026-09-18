//! L0 port contract.
//!
//! **The same assertions run against every backend.** That is the acceptance
//! criterion for the ports being real abstractions rather than descriptions of
//! the in-memory implementation: `run_suite` takes trait objects, so any
//! adapter implementing the ports is fed through it unchanged.
//!
//! Each case uses a freshly generated tenant so the suite is safe to run
//! repeatedly against a persistent backend without cleanup between passes.

use std::sync::Arc;
use std::time::Duration;

use nova_responses::protocol::{CreateResponseRequest, MetadataValue, ProtocolLimits, ResponseItem};
use nova_responses::{
    canonical_items, AgentId, AppendEvent, Attempt, ContextAnchor, Conversation,
    ConversationEventKind, ConversationId, EventBody, IdempotencyKey, ModelParams, NodeTag,
    ResponseEventKind, ResponseId, ResponseRecord, ResponseStatus, TenantId, TurnCommit, TurnSpec,
    Usage,
};
use nova_responses::ports::{
    ContentIntegrity, ConversationError, ConversationStore, CreateOutcome, EventLogError,
    LedgerError, ResponseClaimSource, ResponseEventLog, ResponseIntake,
};

/// The set of ports under test. Backend-agnostic by construction.
#[derive(Clone)]
pub struct PortSet {
    /// Ingress-side ledger (create / cancel / delete / get). The same backend is
    /// usually mounted on both fields — `claims` exists separately so a host whose
    /// execution is driven by its own task system can verify that side alone.
    pub intake: Arc<dyn ResponseIntake>,
    pub claims: Arc<dyn ResponseClaimSource>,
    pub event_log: Arc<dyn ResponseEventLog>,
    pub conversation: Arc<dyn ConversationStore>,
    pub integrity: Option<Arc<dyn ContentIntegrity>>,
    pub node_tag: NodeTag,
}

impl PortSet {
    fn new_id(&self) -> ResponseId {
        ResponseId::new(self.node_tag.clone())
    }

    /// A persisted conversation for `tenant`, with no tail yet.
    async fn fresh_conversation(&self, tenant: &TenantId) -> Conversation {
        self.conversation
            .create(Conversation::new(
                ConversationId::new(),
                tenant.clone(),
                Default::default(),
                1_000,
            ))
            .await
            .expect("create conversation")
    }

}

/// Unique tenant per case, so a persistent backend needs no truncation between
/// runs and cases cannot interfere with one another.
fn fresh_tenant(prefix: &str) -> TenantId {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    TenantId::parse(&format!("{prefix}-{}", &suffix[..12])).expect("generated tenant is valid")
}

fn fresh_key() -> IdempotencyKey {
    IdempotencyKey::parse(&uuid::Uuid::new_v4().to_string()).expect("a uuid is a valid key")
}

fn record(
    id: &ResponseId,
    previous: Option<&ResponseId>,
    tenant: &TenantId,
    stored: bool,
    status: ResponseStatus,
) -> ResponseRecord {
    // The anchor is one field, so "continues a chain" and "belongs to a conversation" are
    // mutually exclusive by construction; cases that need the conversation form overwrite
    // `spec.anchor` rather than setting a second field.
    let anchor = match previous {
        Some(previous) => ContextAnchor::Previous(previous.clone()),
        None => ContextAnchor::Root,
    };
    let mut record = ResponseRecord::queued(
        id.clone(),
        tenant.clone(),
        TurnSpec {
            params: ModelParams {
                instructions: Some("INSTRUCTIONS-MARKER".into()),
                ..ModelParams::new("test-model")
            },
            input_items: vec![ResponseItem::user_text(format!("in-{}", id.uuid()))],
            store: stored,
            ext: None,
            anchor,
        },
        fresh_key(),
        1_000,
        // Retention only matters to the sweep cases, which set their own deadline.
        0,
    );
    record.status = status;
    record.idempotency_key = None;
    record
}

/// A delta event, which is what most log cases append: the content is irrelevant, only
/// the numbering and the fence are under test.
fn event(id: &ResponseId, kind: ResponseEventKind, payload: &str) -> AppendEvent {
    fenced_event(id, kind, None, payload)
}

/// The same, carrying an explicit fence.
///
/// Events are built through the domain's constructors, so a kind can never be paired with
/// a body that does not belong to it — which is why the fence is a parameter here instead
/// of a field the case assigns afterwards.
fn fenced_event(
    id: &ResponseId,
    kind: ResponseEventKind,
    attempt: Option<Attempt>,
    payload: &str,
) -> AppendEvent {
    AppendEvent::from_parts(
        id.clone(),
        kind,
        attempt,
        EventBody::Delta {
            item_id: String::new(),
            output_index: 0,
            content_index: None,
            delta: payload.to_string(),
        },
    )
}

// ---------------------------------------------------------------- event log

/// Event log contract.
///
/// - **CR-5 / INV-11**: sequence numbers are 0-based and contiguous.
/// - **FR-10 / CR-4**: exclusive cursors — resuming at `starting_after=N` yields
///   no repeat and no gap.
/// - **FR-12 / INV-40**: an evicted position fails explicitly, with no partial
///   data and no recovery path.
pub async fn assert_event_log_conformance(log: Arc<dyn ResponseEventLog>, node_tag: &NodeTag) {
    let id = ResponseId::new(node_tag.clone());

    // Sequence numbers start at 0 — not 1 — because `starting_after` needs a
    // cursor space where "before the first event" is representable.
    let first = log
        .append(event(&id, ResponseEventKind::Created, ""))
        .await
        .expect("append first");
    assert_eq!(first, 0, "sequence numbers must be 0-based");

    for expected in 1..5u64 {
        let seq = log
            .append(event(&id, ResponseEventKind::OutputTextDelta, "x"))
            .await
            .expect("append");
        assert_eq!(seq, expected, "sequence numbers must be contiguous");
    }

    let all = log.read_after(&id, None, 100, Duration::from_millis(0)).await.expect("read all");
    let seqs: Vec<u64> = all.iter().map(|e| e.sequence_number()).collect();
    assert_eq!(seqs, vec![0, 1, 2, 3, 4], "no gaps and no repeats");

    // `Some(0)` must skip event 0; `None` must include it. If these collapsed,
    // resuming from the very first event would be impossible.
    let after_zero = log.read_after(&id, Some(0), 100, Duration::from_millis(0)).await.expect("read");
    assert_eq!(
        after_zero.first().map(|e| e.sequence_number()),
        Some(1),
        "starting_after is exclusive"
    );
    let from_start = log.read_after(&id, None, 100, Duration::from_millis(0)).await.expect("read");
    assert_eq!(from_start.first().map(|e| e.sequence_number()), Some(0));

    // Beyond the tip: empty, not an error — the response may still be running.
    let future = log.read_after(&id, Some(999), 10, Duration::from_millis(0)).await.expect("read");
    assert!(future.is_empty());

    // Unknown ids are reported, never treated as an empty stream.
    let unknown = ResponseId::new(node_tag.clone());
    assert_eq!(
        log.read_after(&unknown, None, 10, Duration::from_millis(0)).await,
        Err(EventLogError::Unknown)
    );

    // After the retention window: explicit expiry, and crucially **no partial
    // data and no fallback layer** (INV-40).
    log.close(&id, 10_000, Duration::from_millis(1_000)).await.expect("close");
    log.sweep_expired(11_001).await.expect("sweep");
    assert_eq!(
        log.read_after(&id, None, 10, Duration::from_millis(0)).await,
        Err(EventLogError::Expired)
    );
    assert_eq!(
        log.read_after(&id, Some(2), 10, Duration::from_millis(0)).await,
        Err(EventLogError::Expired),
        "expiry applies to every cursor, not just the earliest"
    );
}

// ------------------------------------------------------------------- ledger

/// Ledger contract.
///
/// - **FR-3 / CR-2 / INV-2**: one idempotency key yields one response, with no
///   expiry window on the key.
/// - **FR-4 / CR-1 / INV-1 / INV-5**: claiming is atomic and raises `attempt`
///   monotonically.
/// - **FR-6**: a superseded attempt is refused. This is the advisory probe; the
///   fenced *write* is covered by `assert_output_provenance`, because an
///   implementation could pass the probe and still accept the append after it.
/// - **FR-8**: the full object is retrievable — status, usage, instruction echo.
pub async fn assert_ledger_conformance(ports: &PortSet) {
    let tenant = fresh_tenant("ledger");
    let id = ports.new_id();
    let key = fresh_key();

    let outcome = ports
        .intake
        .create(
            record(&id, None, &tenant, true, ResponseStatus::Queued),
            key.clone(),
            1_000,
        )
        .await
        .expect("create");
    assert_eq!(
        outcome.record().map(|r| r.response_id.clone()),
        Some(id.clone()),
        "an accepted create returns the stored record"
    );

    // Replay far in the future still returns the original: the gate has no TTL
    // window, so a late retry cannot produce a second response (INV-2).
    let replay = ports
        .intake
        .create(
            record(&ports.new_id(), None, &tenant, true, ResponseStatus::Queued),
            key,
            u64::MAX,
        )
        .await
        .expect("replay");
    assert_eq!(
        replay.record().map(|r| r.response_id.clone()),
        Some(id.clone()),
        "an idempotent replay returns the original record"
    );

    let agent = AgentId::new();
    let claimed = ports
        .claims
        .claim(agent, 2_000, Duration::from_millis(60_000))
        .await
        .expect("claim")
        .expect("something claimable");
    assert_eq!(claimed.record.attempt, Attempt(1), "attempt starts at 1 and increments");
    let claimed_id = claimed.record.response_id.clone();

    // The fence accepts the current attempt and rejects anything else.
    ports
        .claims
        .check_attempt(&claimed_id, claimed.record.attempt)
        .await
        .expect("current attempt is valid");
    assert_eq!(
        ports
            .claims
            .check_attempt(&claimed_id, Attempt(claimed.record.attempt.0 + 1))
            .await,
        Err(LedgerError::StaleAttempt)
    );

    ports
        .claims
        .complete(
            &claimed_id,
            claimed.record.attempt,
            ResponseStatus::Completed,
            Usage::new(3, 4),
            3_000,
        )
        .await
        .expect("complete");

    // Completing twice must fail: the second call is either a duplicate delivery
    // or a superseded holder.
    assert!(ports
        .claims
        .complete(
            &claimed_id,
            claimed.record.attempt,
            ResponseStatus::Completed,
            Usage::default(),
            3_100,
        )
        .await
        .is_err());

    // A non-terminal target status is a programming error, not a transition.
    assert!(matches!(
        ports
            .claims
            .complete(
                &claimed_id,
                claimed.record.attempt,
                ResponseStatus::InProgress,
                Usage::default(),
                3_200,
            )
            .await,
        Err(LedgerError::InvalidTransition(_))
    ));

    let fetched = ports
        .intake
        .get(&claimed_id)
        .await
        .expect("get")
        .expect("present");
    assert_eq!(fetched.status, ResponseStatus::Completed);
    assert_eq!(fetched.usage.total_tokens(), 7);
}

/// Cancellation contract.
///
/// - **FR-7 / INV-51**: cancelling reaches a terminal state.
/// - **INV-51**: the abandoned attempt still books what it consumed. Only the
///   *error* contract is checkable here — see the comment at the booking call for
///   why CR-11 itself cannot be substantiated at the port boundary.
/// - **SEC-2**: a foreign tenant's cancel reads as absent, not as forbidden, so
///   ids cannot be probed for existence.
pub async fn assert_cancel_conformance(ports: &PortSet) {
    let tenant = fresh_tenant("cancel");
    let id = ports.new_id();
    ports
        .intake
        .create(
            record(&id, None, &tenant, true, ResponseStatus::Queued),
            fresh_key(),
            1_000,
        )
        .await
        .expect("create");

    // Foreign tenants are told "not found", never "forbidden" (SEC-2).
    assert_eq!(
        ports
            .intake
            .cancel(&fresh_tenant("intruder"), &id, 1_500)
            .await,
        Err(LedgerError::NotFound)
    );

    ports
        .claims
        .record_partial_usage(&id, Attempt(1), Usage::new(9, 1))
        .await
        .expect("record partial usage");

    // Note what cannot be asserted here, and why.
    //
    // `record_partial_usage` files the amount under `(response_id, attempt)` in a
    // side table — deliberately, since the record's own `usage` belongs to the
    // attempt that completes. But the ledger port exposes **no method to read
    // that side table back**: `partial_usage_count` exists only on the concrete
    // mem adapter. So a billing consumer holding only the port cannot retrieve
    // what was booked, and this suite cannot check that it was.
    //
    // CR-11 is therefore claimed by the L1 `partial-usage-accounted` scenario,
    // which observes the amount through the trace, and **not** by this case. What
    // remains verifiable at the port boundary is the error contract:
    let missing = ResponseId::new(ports.node_tag.clone());
    assert!(
        matches!(
            ports
                .claims
                .record_partial_usage(&missing, Attempt(1), Usage::new(1, 1))
                .await,
            Err(LedgerError::NotFound)
        ),
        "booking usage against an unknown response must fail rather than create a \
         dangling charge"
    );
    ports.intake.cancel(&tenant, &id, 2_000).await.expect("cancel");

    let after = ports.intake.get(&id).await.expect("get").expect("present");
    assert_eq!(after.status, ResponseStatus::Cancelled);
    assert!(after.status.is_terminal());

    // INV-60: cancelling raises the attempt fence, so the (now void) executing agent
    // observes `StaleAttempt` — both its next append and its active cancellation probe.
    assert_eq!(
        ports
            .claims
            .check_attempt(&id, Attempt::UNCLAIMED)
            .await,
        Err(LedgerError::StaleAttempt),
        "cancel must raise the attempt fence so the executor observes it"
    );

    // Cancelling twice is a conflict, not a silent no-op.
    assert!(matches!(
        ports.intake.cancel(&tenant, &id, 2_100).await,
        Err(LedgerError::InvalidTransition(_))
    ));
}

// ------------------------------------------------------------------ context

/// Context store and conversation snapshot contract (D30).
///
/// - **FR-16 / CR-9**: history accumulates in the conversation snapshot,
///   deterministically and in chronological order.
/// - **FR-19 / INV-49**: instructions do not cross turns — they live on the
///   record, never in the snapshot.
/// - **FR-21**: single-response deletion leaves the snapshot intact, and
///   tenant-wide purge is tenant-scoped.
/// - **INV-42 / SEC-2 / SEC-3**: tenancy is checked on snapshot reads; a foreign
///   conversation reads as absent rather than forbidden.
/// - **INV-61**: `append_turn` is idempotent per `response_id`, and a turn with
///   empty output still archives its input (incomplete-turn archival).
pub async fn assert_context_conformance(ports: &PortSet) {
    let tenant = fresh_tenant("ctx");
    let store = &ports.conversation;

    // A conversation's snapshot accumulates turns in order (D30).
    let conversation = store
        .create(Conversation::new(
            ConversationId::new(),
            tenant.clone(),
            Default::default(),
            1_000,
        ))
        .await
        .expect("create conversation");

    let mut ids = Vec::new();
    for i in 0..3 {
        let id = ports.new_id();
        store
            .append_turn(
                &tenant,
                &conversation.id,
                &id,
                TurnCommit {
                    input_items: vec![ResponseItem::user_text(format!("in-{i}"))],
                    output_items: vec![ResponseItem::assistant_text("answer")],
                    reasoning: None,
                    usage: Usage::new(1, 2),
                    status: ResponseStatus::Completed,
                },
                2_000,
            )
            .await
            .expect("append turn");
        ids.push(id);
    }

    let resolved = store
        .read_snapshot(&tenant, &conversation.id)
        .await
        .expect("read snapshot");
    assert_eq!(resolved.turns, 3);
    assert!(resolved.bytes() > 0);
    assert_eq!(resolved.item_count(), 6, "each turn contributes input + output");

    // Chronological order: the oldest turn's input comes first.
    let encoded_first = canonical_items(&resolved.clone().into_items()[..1]);
    assert!(
        encoded_first.contains("in-0"),
        "snapshot must be oldest-first, got {encoded_first}"
    );

    // Instructions must never cross into the snapshot (INV-49): they live on the
    // record, and `append_turn` only takes items.
    let encoded_all = canonical_items(&resolved.clone().into_items());
    assert!(
        !encoded_all.contains("INSTRUCTIONS-MARKER"),
        "instructions leaked into the snapshot"
    );

    // Foreign tenant reads as absent, not forbidden (SEC-2).
    assert_eq!(
        store
            .read_snapshot(&fresh_tenant("other"), &conversation.id)
            .await,
        Err(ConversationError::NotFound)
    );
    let ghost = ConversationId::new();
    assert_eq!(
        store.read_snapshot(&tenant, &ghost).await,
        Err(ConversationError::NotFound)
    );

    // Snapshot survives a response-record deletion (D30): deleting a response
    // removes its ledger record, not the content the conversation inherited.
    ports
        .intake
        .delete(&ids[0])
        .await
        .expect("delete first response record");
    let after_delete = store
        .read_snapshot(&tenant, &conversation.id)
        .await
        .expect("snapshot must survive response deletion");
    assert_eq!(after_delete.turns, 3);
    assert_eq!(after_delete.item_count(), 6);

    // Idempotency per response_id (D30 incomplete-turn archival): the runtime's
    // terminal funnel and the service layer's cancel/reap funnel can both attempt the
    // same turn; the repeat must return the assigned index and add nothing.
    let duplicate = store
        .append_turn(
            &tenant,
            &conversation.id,
            &ids[0],
            TurnCommit {
                input_items: vec![ResponseItem::user_text("in-0")],
                output_items: vec![ResponseItem::assistant_text("answer")],
                reasoning: None,
                usage: Usage::new(1, 2),
                status: ResponseStatus::Completed,
            },
            2_000,
        )
        .await
        .expect("repeat append must succeed");
    assert_eq!(
        duplicate, 0,
        "a repeat append returns the assigned index, not a new turn"
    );
    let after_duplicate = store
        .read_snapshot(&tenant, &conversation.id)
        .await
        .expect("read after duplicate");
    assert_eq!(after_duplicate.turns, 3, "a repeat append must not add a turn");
    assert_eq!(
        after_duplicate.item_count(),
        6,
        "a repeat append must not duplicate items"
    );

    // A turn that ended without output (failed/cancelled/reaped) still archives its
    // input, with empty `output_items` (INV-61): the chain keeps the question.
    let input_only = ports.new_id();
    store
        .append_turn(
            &tenant,
            &conversation.id,
            &input_only,
            TurnCommit {
                input_items: vec![ResponseItem::user_text("unanswered")],
                output_items: vec![],
                reasoning: None,
                usage: Usage::default(),
                status: ResponseStatus::Failed,
            },
            2_000,
        )
        .await
        .expect("input-only append must be legal");
    let after_input_only = store
        .read_snapshot(&tenant, &conversation.id)
        .await
        .expect("read after input-only turn");
    assert_eq!(after_input_only.turns, 4);
    assert_eq!(after_input_only.item_count(), 7);
    assert!(
        canonical_items(&after_input_only.clone().into_items()).contains("unanswered"),
        "the input-only turn's question must be archived"
    );

    // Bulk purge is tenant-scoped.
    let foreign_tenant = fresh_tenant("foreign");
    let foreign_conv = store
        .create(Conversation::new(
            ConversationId::new(),
            foreign_tenant.clone(),
            Default::default(),
            1_000,
        ))
        .await
        .expect("create foreign conversation");
    store
        .append_turn(
            &foreign_tenant,
            &foreign_conv.id,
            &ports.new_id(),
            TurnCommit {
                input_items: vec![ResponseItem::user_text("theirs")],
                output_items: vec![ResponseItem::assistant_text("answer")],
                reasoning: None,
                usage: Usage::new(1, 2),
                status: ResponseStatus::Completed,
            },
            2_000,
        )
        .await
        .expect("append foreign turn");

    let purged = store.delete_by_tenant(&tenant).await.expect("purge");
    assert!(purged >= 1);
    assert_eq!(
        store
            .read_snapshot(&foreign_tenant, &foreign_conv.id)
            .await
            .expect("foreign snapshot survives")
            .turns,
        1,
        "purge must not touch other tenants"
    );

    store.health().await.expect("health");
}

// ---------------------------------------------------------------- integrity

/// Integrity contract.
///
/// - **FR-39 / CR-13**: a tampered payload fails verification.
/// - **INV-44**: canonicalisation is stable across Unicode normalisation forms, so
///   an unmodified record never fails its own check.
pub async fn assert_integrity_conformance(integrity: Arc<dyn ContentIntegrity>) {
    let items = vec![ResponseItem::user_text("hello")];
    let canonical = canonical_items(&items);
    let tag = integrity.sign(&canonical).expect("sign");
    integrity.verify(&canonical, &tag).expect("verify");

    let tampered = canonical_items(&[ResponseItem::user_text("hell0")]);
    assert!(
        integrity.verify(&tampered, &tag).is_err(),
        "modified content must not verify"
    );
    assert!(integrity.verify(&canonical, "deadbeef").is_err());
    assert!(!integrity.alg().is_empty(), "algorithm must be recorded");

    // Canonicalisation makes the tag independent of Unicode composition, so
    // equivalent content does not produce spurious mismatches.
    let composed = canonical_items(&[ResponseItem::user_text("caf\u{00e9}")]);
    let decomposed = canonical_items(&[ResponseItem::user_text("cafe\u{0301}")]);
    assert_eq!(composed, decomposed);
}

// ------------------------------------------------------------ protocol subset

/// Chain closure.
///
/// **FR-28 / CR-12 / INV-47**: every item we can emit must be acceptable as
/// input on the next turn.
///
/// This is the self-inflicted failure the closed subset makes possible: if the
/// execution side produced a type the input validator rejects, our own chain
/// would break with no external caller involved.
pub fn assert_output_items_are_valid_input() {
    let emittable = [
        ResponseItem::assistant_text("text"),
        ResponseItem::FunctionCall {
            call_id: "call_1".into(),
            name: "tool".into(),
            arguments: "{}".into(),
            id: None,
            status: None,
        },
        ResponseItem::FunctionCallOutput {
            call_id: "call_1".into(),
            output: "result".into(),
            id: None,
            status: None,
        },
    ];

    for item in &emittable {
        assert!(
            item.is_acceptable_as_input(),
            "`{}` is emitted but not accepted as input",
            item.item_type()
        );
        // And it must genuinely survive a round trip through the input parser.
        let json = serde_json::to_string(item).expect("serialise");
        let request = format!(r#"{{"model":"m","input":[{json}]}}"#);
        let parsed: Result<CreateResponseRequest, _> = serde_json::from_str(&request);
        assert!(
            parsed.is_ok(),
            "output item `{}` is not parseable as input: {:?}",
            item.item_type(),
            parsed.err()
        );
    }
}

/// Protocol subset.
///
/// - **FR-24 / INV-50**: unknown request fields are refused, never ignored.
/// - **FR-25**: out-of-subset item types are refused.
/// - **FR-26 / SEC-6**: inline binary is refused; references must be https and
///   must not resolve to internal address space.
/// - **FR-27 / SEC-7 / INV-52**: request size ceilings are enforced.
pub fn assert_protocol_subset_rejects() {
    let cases: &[(&str, &str)] = &[
        (
            "unknown request field",
            r#"{"model":"m","input":"hi","truncation":"auto"}"#,
        ),
        (
            "item reference (would bypass per-hop tenant checks)",
            r#"{"model":"m","input":[{"type":"item_reference","id":"msg_1"}]}"#,
        ),
        (
            "reasoning item",
            r#"{"model":"m","input":[{"type":"reasoning","summary":[]}]}"#,
        ),
        (
            "hosted tool",
            r#"{"model":"m","input":"hi","tools":[{"type":"web_search"}]}"#,
        ),
        (
            "unknown content part",
            r#"{"model":"m","input":[{"type":"message","role":"user","content":[{"type":"input_audio","data":"AA"}]}]}"#,
        ),
        (
            "unknown field inside a known item",
            r#"{"model":"m","input":[{"type":"message","role":"user","content":[],"extra":1}]}"#,
        ),
    ];
    for (label, body) in cases {
        assert!(
            serde_json::from_str::<CreateResponseRequest>(body).is_err(),
            "{label} must be rejected"
        );
    }

    // Inline binary parses structurally but is stopped by the URL guard, so the
    // rejection has to happen during validation.
    let inline: CreateResponseRequest = serde_json::from_str(
        r#"{"model":"m","input":[{"type":"message","role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,AAAA"}]}]}"#,
    )
    .expect("structurally valid");
    assert!(
        inline.validate(&ProtocolLimits::default()).is_err(),
        "inline binary must be rejected"
    );

    let internal: CreateResponseRequest = serde_json::from_str(
        r#"{"model":"m","input":[{"type":"message","role":"user","content":[{"type":"input_image","image_url":"https://169.254.169.254/x"}]}]}"#,
    )
    .expect("structurally valid");
    assert!(
        internal.validate(&ProtocolLimits::default()).is_err(),
        "internal addresses must be rejected"
    );

    // And the supported shapes must actually work, otherwise "strict" would just
    // mean "broken".
    let ok: CreateResponseRequest =
        serde_json::from_str(r#"{"model":"m","input":"hi","store":true,"temperature":0.5}"#)
            .expect("supported request");
    assert!(ok.validate(&ProtocolLimits::default()).is_ok());
    assert!(ok.store, "store defaults to true");
}

/// Event coalescing.
///
/// **INV-16**: only text deltas may be coalesced; envelope events never may,
/// because collapsing them would destroy the sequence a resuming subscriber
/// depends on.
pub fn assert_event_coalescing() {
    assert!(ResponseEventKind::OutputTextDelta.coalescible());
    for kind in [
        ResponseEventKind::Created,
        ResponseEventKind::InProgress,
        ResponseEventKind::Completed,
        ResponseEventKind::Failed,
        ResponseEventKind::Incomplete,
    ] {
        assert!(!kind.coalescible(), "{kind:?} must not be coalescible");
    }
    // Terminal classification drives the retention window, so pin it here too.
    assert!(ResponseEventKind::Completed.is_terminal());
    assert!(!ResponseEventKind::OutputTextDelta.is_terminal());
}

// --------------------------------------------------------------- global claim

/// D25: any execution process may claim any queued response (FR-4).
///
/// Replaces the D23 `claim-locality` case. The in-flight buffer is now shared,
/// so there is no "wrong process" for increments to land in — a node filter
/// would instead strand queued responses on other nodes. The
/// two-handed assertion also verifies CR-1: each response is handed out exactly
/// once, never to two concurrent claimers.
pub async fn assert_global_claim(ports: &PortSet) {
    let tenant = fresh_tenant("global-claim");

    // Drain the queue so the assertion is about what follows.
    loop {
        let Some(c) = ports
            .claims
            .claim(AgentId::new(), 1_000, Duration::from_millis(30_000))
            .await
            .expect("drain claim")
        else {
            break;
        };
        ports
            .claims
            .complete(
                &c.record.response_id,
                c.record.attempt,
                ResponseStatus::Completed,
                Usage::default(),
                1_000,
            )
            .await
            .expect("drain complete");
    }

    // Two queued responses with different node tags.
    let other_node = NodeTag::parse("node-zz").expect("static tag");
    let foreign_id = ResponseId::new(other_node.clone());
    let foreign = record(&foreign_id, None, &tenant, true, ResponseStatus::Queued);
    ports
        .intake
        .create(foreign, fresh_key(), 1_000)
        .await
        .expect("create foreign");

    let mine = ports.new_id();
    ports
        .intake
        .create(
            record(&mine, None, &tenant, true, ResponseStatus::Queued),
            fresh_key(),
            1_100,
        )
        .await
        .expect("create local");

    // One execution process claims both, in FIFO order, regardless of node tag.
    let first = ports
        .claims
        .claim(AgentId::new(), 2_000, Duration::from_millis(30_000))
        .await
        .expect("claim first")
        .expect("something claimable");
    let second = ports
        .claims
        .claim(AgentId::new(), 2_100, Duration::from_millis(30_000))
        .await
        .expect("claim second")
        .expect("something claimable");

    // Both responses are handed out, and each gets a fresh attempt-1 fence.
    let mut claimed_ids = [
        first.record.response_id.clone(),
        second.record.response_id.clone(),
    ];
    claimed_ids.sort();
    let mut expected = [foreign_id, mine];
    expected.sort();
    assert_eq!(
        claimed_ids, expected,
        "global claim must hand out every queued response regardless of node tag"
    );
    assert_eq!(first.record.attempt.0, 1);
    assert_eq!(second.record.attempt.0, 1);

    // Drive both claims to a terminal state before returning, so in-flight work
    // left behind cannot consume another case's admission budget.
    for c in [&first, &second] {
        ports
            .claims
            .complete(
                &c.record.response_id,
                c.record.attempt,
                ResponseStatus::Completed,
                Usage::default(),
                2_200,
            )
            .await
            .expect("release claimed work");
    }
}

// -------------------------------------------------------- output provenance

/// FR-20 / INV-48 / INV-6 / CR-3 / CR-7: stored output comes from the executor's
/// terminal submission, never from replaying the event stream (INV-48).
///
/// This is the load-bearing decision of the whole design (D20), and it had no
/// verification at all — the id was listed against the cancel case, which does not
/// touch it. Left unchecked, the cheapest way to "fix" a future bug would be to
/// rebuild output items by replaying deltas, which is precisely what the event
/// buffer's bounded, transient nature makes unsound: it may be evicted at any
/// time, so deriving durable content from it would make history depend on a cache.
///
/// Verified by its decisive consequence: **destroy the event stream, and the
/// stored output must still be complete.**
pub async fn assert_output_provenance(ports: &PortSet) {
    let tenant = fresh_tenant("provenance");
    let id = ports.new_id();
    let conversation = ports
        .conversation
        .create(Conversation::new(
            ConversationId::new(),
            tenant.clone(),
            Default::default(),
            1_000,
        ))
        .await
        .expect("create conversation");

    // The executor streams deltas for the caller's benefit...
    for chunk in ["Sta", "ble ", "answer"].iter() {
        let ev = event(&id, ResponseEventKind::OutputTextDelta, chunk);
        ports.event_log.append(ev).await.expect("append delta");
    }

    // ...and separately submits the terminal items to the conversation snapshot.
    // Two write paths, deliberately.
    ports
        .conversation
        .append_turn(
            &tenant,
            &conversation.id,
            &id,
            TurnCommit {
                input_items: vec![ResponseItem::user_text("question")],
                output_items: vec![ResponseItem::assistant_text("Stable answer")],
                reasoning: None,
                usage: Usage::new(3, 4),
                status: ResponseStatus::Completed,
            },
            2_000,
        )
        .await
        .expect("append_turn");

    // Now discard the event stream entirely, as eviction or a node restart would.
    ports
        .event_log
        .close(&id, 3_000, Duration::from_millis(1_000))
        .await
        .expect("close");
    // Strictly past the retention window, matching the event-log case's convention.
    ports.event_log.sweep_expired(4_001).await.expect("sweep");
    assert!(
        matches!(
            ports.event_log.read_after(&id, None, 64, Duration::from_millis(0)).await,
            Err(EventLogError::Expired)
        ),
        "the event stream must be genuinely gone for this check to mean anything"
    );

    // FR-20: the durable snapshot is untouched by that loss.
    let snapshot = ports
        .conversation
        .read_snapshot(&tenant, &conversation.id)
        .await
        .expect("snapshot read");
    assert!(
        canonical_items(&snapshot.clone().into_items()).contains("Stable answer"),
        "output items must come from the executor's terminal submission (FR-20); \
         if they were derived by replaying the event stream, discarding that \
         stream would have emptied them, making durable history depend on a \
         bounded transient buffer"
    );

    // CR-7 / INV-6: a superseded holder cannot inject events afterwards. Checked
    // here against the real append path rather than the advisory pre-check, since
    // it is the write that must be fenced — an implementation could pass a
    // `check_attempt` probe and still accept the write that follows it.
    let fenced = ports.new_id();
    ports
        .intake
        .create(
            record(&fenced, None, &tenant, true, ResponseStatus::Queued),
            fresh_key(),
            1_000,
        )
        .await
        .expect("create");
    let claimed = ports
        .claims
        .claim(AgentId::new(), 1_100, Duration::from_millis(30_000))
        .await
        .expect("claim")
        .expect("something was queued");
    let live = claimed.record.attempt;

    ports
        .event_log
        .append(fenced_event(
            &claimed.record.response_id,
            ResponseEventKind::OutputTextDelta,
            Some(live),
            "live",
        ))
        .await
        .expect("the current holder must be able to write");

    let stale = Attempt(live.0.saturating_sub(1));
    let rejected = ports
        .event_log
        .append(fenced_event(
            &claimed.record.response_id,
            ResponseEventKind::OutputTextDelta,
            Some(stale),
            "from a reaped holder",
        ))
        .await;
    assert!(
        matches!(rejected, Err(EventLogError::StaleAttempt)),
        "an append carrying a superseded attempt must be rejected, got {rejected:?}. \
         CR-3: without this fence a reaped executor's output interleaves with the \
         new attempt's, and the subscriber sees two answers spliced together."
    );
}

// ------------------------------------------------------------ durability order

/// INV-34 / CR-6: a success response is never returned before the write landed.
///
/// The failure this rules out is the most expensive kind to diagnose: the caller
/// is told the generation exists, retries nothing, and the record is discovered
/// missing on a later turn as a broken chain — arbitrarily far from the request
/// that actually failed.
///
/// Verified through its only externally observable consequence: the moment
/// `create` reports `Accepted`, the content must already be readable. Any
/// implementation that acknowledges first and persists afterwards fails here.
///
/// **INV-46** is included because the same case rules out a partial write: a
/// replayed create must leave no half-stored second record behind.
pub async fn assert_durability_order(ports: &PortSet) {
    let tenant = fresh_tenant("durability");
    let id = ports.new_id();
    let mut rec = record(&id, None, &tenant, true, ResponseStatus::Queued);
    rec.spec.input_items = vec![ResponseItem::user_text("durable-marker")];

    let outcome = ports
        .intake
        .create(rec, fresh_key(), 1_000)
        .await
        .expect("create");
    assert!(matches!(outcome, CreateOutcome::Accepted(_)));

    // No sleep, no retry loop: "eventually visible" is precisely what this
    // invariant forbids.
    let seen = ports
        .intake
        .get(&id)
        .await
        .expect("ledger get")
        .expect("a response reported as accepted must be readable immediately");
    assert_eq!(seen.response_id, id);
    assert!(
        seen.is_stored(),
        "the record must retain store=true, otherwise the chain it anchors cannot \
         be resolved later"
    );

    // The input items must be visible on the record the instant create() succeeds
    // (D30): acknowledging before the write means a later turn discovers a broken
    // anchor far from the request that actually failed (INV-34).
    assert!(
        canonical_items(&seen.spec.input_items).contains("durable-marker"),
        "the persisted input items must be the ones submitted"
    );

    // A rejected create must leave nothing behind: a partial write would be a
    // silent inconsistency that no error message accounts for.
    let ghost = ports.new_id();
    let ghost_rec = record(&ghost, None, &tenant, true, ResponseStatus::Queued);
    let key = fresh_key();
    ports
        .intake
        .create(ghost_rec.clone(), key.clone(), 1_000)
        .await
        .expect("first create");
    // Replaying the same key must not produce a second stored record.
    let replay = ports
        .intake
        .create(ghost_rec, key, 1_000)
        .await
        .expect("replay");
    assert!(
        matches!(replay, CreateOutcome::Duplicate(_)),
        "a replayed key must be reported as duplicate, not stored twice"
    );
}

// ---------------------------------------------------------------- concurrency

/// CR-1 / INV-1 / INV-2 / INV-11 under genuine contention.
///
/// Every other case in this suite drives the ports sequentially, which cannot
/// distinguish "atomic" from "happens to work when nothing else is running".
/// Exactly-once claiming, idempotency and sequence allocation are all
/// *concurrency* properties: a check-then-act implementation passes every
/// sequential test and still double-claims in production.
///
/// The tasks are spawned so a multi-threaded runtime can interleave them; on a
/// single-threaded runtime they still interleave at await points.
///
/// **CR-5** is claimed here as well as by the event-log case: contiguity *under
/// contention* is a distinct property, and only the concurrent form rules out two
/// appends receiving the same number.
pub async fn assert_concurrency_conformance(ports: &PortSet) {
    const RACERS: usize = 12;

    // --- exactly one claim wins -------------------------------------------
    let tenant = fresh_tenant("conc-claim");
    let id = ports.new_id();
    let created = ports
        .intake
        .create(
            record(&id, None, &tenant, true, ResponseStatus::Queued),
            fresh_key(),
            1_000,
        )
        .await
        .expect("create");
    assert!(matches!(created, CreateOutcome::Accepted(_)));

    let mut handles = Vec::new();
    for _ in 0..RACERS {
        let ledger = ports.claims.clone();
        handles.push(tokio::spawn(async move {
            ledger
                .claim(AgentId::new(), 1_100, Duration::from_millis(30_000))
                .await
        }));
    }

    let mut winners = Vec::new();
    for h in handles {
        if let Some(claimed) = h.await.expect("claim task panicked").expect("claim call") {
            winners.push(claimed);
        }
    }

    // `claim` draws from the node's whole queue, not from a caller-chosen id, so
    // other cases in this suite may legitimately contribute additional winners.
    // The property under test is therefore **not** "one winner overall" — it is
    // that no single response is handed out twice. Asserting the former was wrong
    // and produced a failure that said nothing about correctness.
    let mut claimed_ids: Vec<String> = winners
        .iter()
        .map(|w| w.record.response_id.to_string())
        .collect();
    let distinct: std::collections::BTreeSet<_> = claimed_ids.iter().cloned().collect();
    assert_eq!(
        distinct.len(),
        claimed_ids.len(),
        "no response may be claimed twice; got duplicates in {claimed_ids:?}. A \
         check-then-act claim passes every sequential test and still hands the \
         same response to two agents under load, interleaving their output (CR-1)."
    );

    let mine = claimed_ids.iter().filter(|c| *c == &id.to_string()).count();
    assert_eq!(
        mine, 1,
        "the response queued by this case must be claimed exactly once, not {mine} times"
    );
    for w in &winners {
        assert_eq!(
            w.record.attempt.0, 1,
            "a first claim must produce attempt 1, otherwise the fence cannot \
             distinguish a retry from the original"
        );
    }
    claimed_ids.clear();

    // --- one idempotency key, one response --------------------------------
    let tenant = fresh_tenant("conc-idem");
    let key = fresh_key();
    let mut handles = Vec::new();
    for _ in 0..RACERS {
        let ledger = ports.intake.clone();
        let key = key.clone();
        // Each racer proposes a *different* id, as independent retries would.
        let candidate = ports.new_id();
        let rec = record(&candidate, None, &tenant, true, ResponseStatus::Queued);
        handles.push(tokio::spawn(
            async move { ledger.create(rec, key, 2_000).await },
        ));
    }

    let mut accepted = Vec::new();
    let mut duplicates = Vec::new();
    for h in handles {
        match h.await.expect("create task panicked").expect("create call") {
            CreateOutcome::Accepted(record) => accepted.push(record.response_id.clone()),
            CreateOutcome::Duplicate(record) => duplicates.push(record.response_id.clone()),
        }
    }
    assert_eq!(
        accepted.len(),
        1,
        "one idempotency key must yield one response, not {} (CR-2/INV-2). \
         Concurrent retries are the normal case for a client that timed out.",
        accepted.len()
    );
    assert_eq!(duplicates.len(), RACERS - 1);
    for dup in &duplicates {
        assert_eq!(
            dup, &accepted[0],
            "a duplicate must return the winning response id, otherwise the \
             caller cannot find the generation it paid for"
        );
    }

    // --- sequence allocation has no gaps and no repeats --------------------
    let id = ports.new_id();
    let mut handles = Vec::new();
    for i in 0..RACERS {
        let log = ports.event_log.clone();
        let ev = event(&id, ResponseEventKind::OutputTextDelta, &format!("d{i}"));
        handles.push(tokio::spawn(async move { log.append(ev).await }));
    }
    let mut assigned = Vec::new();
    for h in handles {
        assigned.push(h.await.expect("append task panicked").expect("append call"));
    }
    assigned.sort_unstable();
    let unique: std::collections::BTreeSet<_> = assigned.iter().copied().collect();
    assert_eq!(
        unique.len(),
        assigned.len(),
        "concurrent appends must never receive the same sequence number: two \
         events sharing a number make `starting_after` ambiguous and silently \
         drop one of them on resume (INV-11)"
    );
    assert_eq!(
        assigned,
        (0..RACERS as u64).collect::<Vec<_>>(),
        "sequence numbers must stay 0-based and contiguous under contention; \
         gaps would be indistinguishable from eviction"
    );

    // Readable back in one contiguous run, which is what a resuming subscriber
    // depends on.
    let read = ports
        .event_log
        .read_after(&id, None, 128, Duration::from_millis(0))
        .await
        .expect("read back");
    assert_eq!(read.len(), RACERS);
    for (i, ev) in read.iter().enumerate() {
        assert_eq!(ev.sequence_number(), i as u64);
    }
}

// -------------------------------------------------------------- entry points

// ------------------------------------------------------------- conversation

/// Conversation store contract (D27).
///
/// - **FR-40**: the four upstream operations round-trip; metadata replaces
///   wholesale so a key can be removed; deletion does not cascade.
/// - **FR-42**: the list operation returns this tenant's conversations newest
///   first and never crosses the tenant boundary.
/// - **FR-21**: tenant-level bulk erasure reaches conversations, so a purge is
///   not a false claim.
/// - **INV-54**: the conversation holds a pointer, never items.
/// - **INV-55**: advancing the tail is last-write-wins — no compare-and-set,
///   because the conflict status one would have to return does not exist
///   upstream.
/// - **SEC-2**: another tenant's conversation is indistinguishable from absent,
///   across reads *and* writes.
pub async fn assert_conversation_conformance(ports: &PortSet) {
    let tenant = fresh_tenant("conv");
    let other = fresh_tenant("conv-other");

    let created = ports.fresh_conversation(&tenant).await;
    assert!(
        created.last_response_id.is_none(),
        "a new conversation has no tail: the first turn must start from empty context"
    );

    let fetched = ports
        .conversation
        .get(&tenant, &created.id)
        .await
        .expect("get")
        .expect("present");
    assert_eq!(fetched, created, "retrieve must round-trip the record");

    // SEC-2: a foreign tenant reads as absent, not as forbidden — otherwise the
    // status alone confirms the id exists.
    assert_eq!(
        ports.conversation.get(&other, &created.id).await.expect("get"),
        None,
        "another tenant's conversation must read as absent"
    );
    assert_eq!(
        ports
            .conversation
            .update_metadata(&other, &created.id, Default::default())
            .await,
        Err(ConversationError::NotFound),
        "a foreign tenant must not be able to update"
    );
    assert_eq!(
        ports.conversation.delete(&other, &created.id).await,
        Ok(false),
        "a foreign tenant must not be able to delete"
    );

    // Metadata replaces wholesale, so a key can be removed. A merge-patch could
    // only ever add.
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert(
        "topic".to_string(),
        MetadataValue::String("demo".to_string()),
    );
    metadata.insert("stale".to_string(), MetadataValue::String("x".to_string()));
    let updated = ports
        .conversation
        .update_metadata(&tenant, &created.id, metadata)
        .await
        .expect("update");
    assert_eq!(updated.metadata.len(), 2);

    let mut narrower = std::collections::BTreeMap::new();
    narrower.insert(
        "topic".to_string(),
        MetadataValue::String("demo".to_string()),
    );
    let updated = ports
        .conversation
        .update_metadata(&tenant, &created.id, narrower)
        .await
        .expect("update");
    assert_eq!(
        updated.metadata.len(),
        1,
        "metadata is replaced wholesale, so a key must be removable"
    );
    assert_eq!(
        updated.created_at_ms, created.created_at_ms,
        "updating metadata must not disturb the rest of the record"
    );

    // The tail: what the next generation inherits.
    let first = ports.new_id();
    ports
        .conversation
        .advance(&tenant, &created.id, &first)
        .await
        .expect("advance");
    let after = ports
        .conversation
        .get(&tenant, &created.id)
        .await
        .expect("get")
        .expect("present");
    assert_eq!(after.last_response_id.as_ref(), Some(&first));
    assert!(after.last_response_id.is_some());

    // INV-55: last write wins. Two turns racing on one conversation leave
    // whichever finished last as the tail; the other's chain survives and stays
    // addressable, it simply is not the tail. A compare-and-set here would have
    // to reject the loser with a status upstream never returns.
    let second = ports.new_id();
    ports
        .conversation
        .advance(&tenant, &created.id, &second)
        .await
        .expect("advance again");
    assert_eq!(
        ports
            .conversation
            .get(&tenant, &created.id)
            .await
            .expect("get")
            .expect("present")
            .last_response_id
            .as_ref(),
        Some(&second),
        "advancing must overwrite unconditionally (last write wins)"
    );

    // Advancing a conversation that does not exist is an error, never a silent
    // no-op: the caller would otherwise believe the tail moved.
    assert_eq!(
        ports
            .conversation
            .advance(&tenant, &ConversationId::new(), &second)
            .await,
        Err(ConversationError::NotFound)
    );
    assert_eq!(
        ports
            .conversation
            .advance(&other, &created.id, &second)
            .await,
        Err(ConversationError::NotFound),
        "advancing across a tenant boundary must fail as absent"
    );

    // Delete, and confirm it is gone. Response records are deliberately not
    // cascaded — see the port docs and D24.
    assert_eq!(
        ports.conversation.delete(&tenant, &created.id).await,
        Ok(true)
    );
    assert_eq!(
        ports.conversation.get(&tenant, &created.id).await.expect("get"),
        None
    );
    assert_eq!(
        ports.conversation.delete(&tenant, &created.id).await,
        Ok(false),
        "deleting twice reports no removal rather than an error"
    );

    // Bulk erasure by tenant (FR-21) must reach conversations too, or a purge
    // would leave them behind while reporting success. `a` and `b` get distinct
    // timestamps so the list-order assertion below has a well-defined order.
    let a = ports
        .conversation
        .create(Conversation::new(
            ConversationId::new(),
            tenant.clone(),
            Default::default(),
            1_000,
        ))
        .await
        .expect("create a");
    let b = ports
        .conversation
        .create(Conversation::new(
            ConversationId::new(),
            tenant.clone(),
            Default::default(),
            2_000,
        ))
        .await
        .expect("create b");
    let untouched = ports.fresh_conversation(&other).await;

    // FR-42: list returns this tenant's conversations, newest first, and never
    // leaks another tenant's.
    let listed = ports.conversation.list(&tenant).await.expect("list");
    let ids: Vec<_> = listed.iter().map(|c| c.id.clone()).collect();
    assert!(ids.contains(&a.id), "list must include this tenant's conversations");
    assert!(ids.contains(&b.id), "list must include the second conversation");
    assert!(
        !ids.contains(&untouched.id),
        "list must not return another tenant's conversation"
    );
    let pos_b = ids.iter().position(|id| *id == b.id).expect("b listed");
    let pos_a = ids.iter().position(|id| *id == a.id).expect("a listed");
    assert!(pos_b < pos_a, "list must be newest-first: {ids:?}");

    let removed = ports
        .conversation
        .delete_by_tenant(&tenant)
        .await
        .expect("purge");
    assert!(removed >= 2, "purge must remove this tenant's conversations");
    for id in [&a.id, &b.id] {
        assert_eq!(ports.conversation.get(&tenant, id).await.expect("get"), None);
    }
    assert!(
        ports
            .conversation
            .get(&other, &untouched.id)
            .await
            .expect("get")
            .is_some(),
        "a purge must not cross the tenant boundary"
    );
}

/// Conversation event-stream and turn-lock contract (D28).
///
/// - **INV-58 / CR-14**: acquiring the turn lock and announcing it are atomic —
///   a reader never observes the marker without its `TurnStarted`, and a refused
///   turn leaves no event and no half state.
/// - **FR-44**: a second turn is refused with the holder named.
/// - **CR-15**: releasing is conditional and idempotent — the marker clears
///   exactly once, and a repeat release emits no second event.
/// - **INV-57 / CR-16 / FR-43**: turn boundaries and business events share one
///   0-based contiguous sequence space.
/// - **INV-59**: reaching the per-conversation event bound refuses the append,
///   never evicting the oldest event.
/// - **INV-56**: a conversation event carries references only — its wire form
///   never contains conversation content.
/// - **SEC-2**: the lock is tenant-scoped; a foreign tenant reads as absent.
pub async fn assert_conversation_events_conformance(ports: &PortSet) {
    let tenant = fresh_tenant("conv-ev");
    let other = fresh_tenant("conv-ev-other");
    let conv = ports.fresh_conversation(&tenant).await;
    let cid = conv.id.clone();

    // INV-58 / CR-14: acquire_active sets the marker and emits TurnStarted
    // atomically. A reader must never observe one without the other.
    let r1 = ports.new_id();
    let start_seq = ports
        .conversation
        .acquire_active(&tenant, &cid, &r1, 1_000)
        .await
        .expect("acquire");
    let held = ports
        .conversation
        .get(&tenant, &cid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held.active_response_id.as_ref(), Some(&r1), "marker must be set");
    let events = ports
        .conversation
        .read_after(&tenant, &cid, None, 10, Duration::from_millis(0))
        .await
        .expect("read");
    assert_eq!(events.len(), 1, "TurnStarted must be emitted with the acquire");
    assert_eq!(events[0].seq, start_seq);
    assert!(matches!(events[0].kind, ConversationEventKind::TurnStarted { .. }));

    // FR-44 / INV-58: a second turn is refused with the holder named, and leaves
    // no event and no half state (CR-14).
    let r2 = ports.new_id();
    match ports.conversation.acquire_active(&tenant, &cid, &r2, 1_100).await {
        Err(ConversationError::Busy { holder }) => assert_eq!(holder, r1),
        other => panic!("expected Busy, got {other:?}"),
    }
    let events = ports
        .conversation
        .read_after(&tenant, &cid, None, 10, Duration::from_millis(0))
        .await
        .unwrap();
    assert_eq!(events.len(), 1, "a refused turn must leave no event behind");

    // Re-entrant: the same holder re-acquiring is an idempotent retry, not a
    // second turn — it returns the original sequence and emits nothing.
    let again = ports
        .conversation
        .acquire_active(&tenant, &cid, &r1, 1_200)
        .await
        .expect("re-entrant acquire");
    assert_eq!(again, start_seq, "re-entrant acquire returns the original seq");
    let events = ports
        .conversation
        .read_after(&tenant, &cid, None, 10, Duration::from_millis(0))
        .await
        .unwrap();
    assert_eq!(events.len(), 1, "re-entrant acquire must not emit a second TurnStarted");

    // INV-58 / CR-15: release_active clears the marker and emits TurnCompleted
    // atomically.
    let done_seq = ports
        .conversation
        .release_active(&tenant, &cid, &r1, ResponseStatus::Completed, 1_300)
        .await
        .expect("release");
    let released = ports
        .conversation
        .get(&tenant, &cid)
        .await
        .unwrap()
        .unwrap();
    assert!(released.active_response_id.is_none(), "marker must be cleared");
    let events = ports
        .conversation
        .read_after(&tenant, &cid, Some(start_seq), 10, Duration::from_millis(0))
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].seq, done_seq);
    assert!(matches!(events[0].kind, ConversationEventKind::TurnCompleted { .. }));

    // CR-15: releasing again is idempotent — no second event, no error.
    let _ = ports
        .conversation
        .release_active(&tenant, &cid, &r1, ResponseStatus::Completed, 1_400)
        .await
        .expect("idempotent release");
    let events = ports
        .conversation
        .read_after(&tenant, &cid, Some(done_seq), 10, Duration::from_millis(0))
        .await
        .unwrap();
    assert!(events.is_empty(), "idempotent release must not emit a second TurnCompleted");

    // INV-57 / CR-16 / FR-43: a business event shares the same contiguous
    // sequence space as the turn boundaries.
    let biz_seq = ports
        .conversation
        .append_event(
            &tenant,
            &cid,
            ConversationEventKind::Business {
                kind: "note".into(),
                payload: serde_json::json!({ "k": "v" }),
            },
            1_500,
        )
        .await
        .expect("business event");
    assert!(
        biz_seq > done_seq,
        "a business event must follow the turn boundary in one sequence space"
    );
    let events = ports
        .conversation
        .read_after(&tenant, &cid, Some(done_seq), 10, Duration::from_millis(0))
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].seq, biz_seq);
    assert!(matches!(events[0].kind, ConversationEventKind::Business { .. }));

    // Full read-back is 0-based and contiguous (INV-57).
    let all = ports
        .conversation
        .read_after(&tenant, &cid, None, 100, Duration::from_millis(0))
        .await
        .unwrap();
    let seqs: Vec<u64> = all.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, vec![start_seq, done_seq, biz_seq], "sequences must be contiguous");

    // release_stale_active clears a terminal holder without an event (its
    // terminal event was already emitted by whoever completed it).
    let r3 = ports.new_id();
    ports
        .conversation
        .acquire_active(&tenant, &cid, &r3, 1_600)
        .await
        .expect("acquire r3");
    let cleared = ports
        .conversation
        .release_stale_active(&tenant, &cid, &r3)
        .await
        .expect("stale release");
    assert!(cleared, "stale holder must be cleared");
    let held = ports
        .conversation
        .get(&tenant, &cid)
        .await
        .unwrap()
        .unwrap();
    assert!(held.active_response_id.is_none());

    // SEC-2: the lock is tenant-scoped — a foreign tenant reads as absent.
    let r4 = ports.new_id();
    assert!(matches!(
        ports.conversation.acquire_active(&other, &cid, &r4, 1_700).await,
        Err(ConversationError::NotFound)
    ));

    // INV-59: reaching the per-conversation event bound refuses the append
    // rather than evicting the oldest event.
    let capped = ports.fresh_conversation(&tenant).await;
    ports.conversation.set_max_events(2);
    for i in 0..2u64 {
        ports
            .conversation
            .append_event(
                &tenant,
                &capped.id,
                ConversationEventKind::Business {
                    kind: "b".into(),
                    payload: serde_json::json!({}),
                },
                2_000 + i,
            )
            .await
            .expect("append within the cap");
    }
    assert!(
        matches!(
            ports
                .conversation
                .append_event(
                    &tenant,
                    &capped.id,
                    ConversationEventKind::Business {
                        kind: "b".into(),
                        payload: serde_json::json!({}),
                    },
                    2_100,
                )
                .await,
            Err(ConversationError::CapacityExceeded)
        ),
        "reaching the event bound must refuse the append, not evict (INV-59)"
    );
    // Restore the default bound so later cases are unaffected.
    ports.conversation.set_max_events(100_000);

    // INV-56: a conversation event carries references only. Its wire form must
    // never contain conversation content — the envelope names a response or a
    // business payload, never the items themselves.
    for kind in [
        ConversationEventKind::TurnStarted {
            response_id: r1.clone(),
        },
        ConversationEventKind::TurnCompleted {
            response_id: r1.clone(),
            status: ResponseStatus::Completed,
        },
        ConversationEventKind::ResponseDeleted {
            response_id: r1.clone(),
        },
        ConversationEventKind::Business {
            kind: "k".into(),
            payload: serde_json::json!({ "x": 1 }),
        },
    ] {
        let obj = serde_json::to_value(&kind).unwrap();
        let obj = obj.as_object().expect("event serialises to an object");
        assert!(
            !obj.contains_key("items"),
            "conversation event must not carry items (INV-56): {obj:?}"
        );
        assert!(
            !obj.contains_key("content"),
            "conversation event must not carry content (INV-56): {obj:?}"
        );
    }
}



/// What a contract case actually exercises.
///
/// The distinction matters for reporting: running a protocol-level case against
/// two backends does not mean the protocol was verified twice, and presenting it
/// that way would overstate backend coverage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseScope {
    /// Exercises a port implementation; the result depends on the backend.
    Backend,
    /// Exercises shared domain/protocol code; identical for every backend.
    Protocol,
    /// Requires an optional port. Skipped — **and reported as skipped** — when
    /// the backend does not supply it.
    OptionalPort,
}

/// One contract case: its name, what it substantiates, and its scope.
#[derive(Debug, Clone, Copy)]
pub struct ContractCase {
    pub name: &'static str,
    pub covers: &'static [&'static str],
    pub scope: CaseScope,
    /// The assertion function that substantiates `covers`.
    ///
    /// Named explicitly so `covers_claims_are_substantiated` can locate the body
    /// and check that every id listed is actually argued for somewhere inside it.
    /// Without this link, `covers` is an unverifiable self-declaration — and the
    /// coverage figure is derived from it.
    pub asserts: &'static str,
}

/// The single source of truth for what the L0 contract contains.
///
/// Both entry points iterate this list, and `xtask coverage` reads `covers` from
/// it. Previously the suite existed as two hand-copied call sequences plus a
/// third hard-coded list of requirement ids inside the coverage gate — so an
/// assertion added in one place, or deleted from all of them, changed nothing
/// visible. A gate that cannot notice its own contents shrinking is not a gate.
pub fn cases() -> &'static [ContractCase] {
    &[
        ContractCase {
            name: "event-log",
            // FR-9 / FR-11 / INV-16 were claimed here and removed: a storage port
            // cannot observe "subscribable stream", "no connection stickiness", or
            // event coalescing. They belong to the ingress layer (L2) and to the
            // event-coalesce case respectively.
            covers: &["FR-10", "FR-12", "CR-4", "CR-5", "INV-11", "INV-40"],
            scope: CaseScope::Backend,
            asserts: "assert_event_log_conformance",
        },
        ContractCase {
            name: "ledger",
            // FR-5 moved to orphan-reclaim (which actually reclaims); CR-3/CR-7/
            // INV-6 moved to output-provenance, which fences the real append path
            // rather than the advisory `check_attempt` probe.
            covers: &["FR-3", "FR-4", "FR-6", "FR-8", "CR-1", "CR-2", "INV-1", "INV-2", "INV-5"],
            scope: CaseScope::Backend,
            asserts: "assert_ledger_conformance",
        },
        ContractCase {
            name: "cancel",
            // FR-20 was claimed here and moved to output-provenance: cancelling a
            // response says nothing about where stored output came from.
            //
            // CR-11 was also claimed here and dropped: the port offers no way to
            // read booked partial usage back, so this case cannot substantiate it.
            // Covered by the L1 `partial-usage-accounted` scenario via the trace.
            covers: &["FR-7", "INV-51", "INV-60", "SEC-2"],
            scope: CaseScope::Backend,
            asserts: "assert_cancel_conformance",
        },
        ContractCase {
            name: "context-chain",
            // FR-15 (store switch), FR-17 (depth/byte ceilings) and FR-18
            // (four break kinds) live in the service layer's `resolve_context`
            // under D30 — a storage port cannot observe them, so they are
            // substantiated by L2 instead. FR-22 (content expiry) is removed
            // with D30 (content is the conversation snapshot's, not a
            // per-response record to sweep).
            covers: &["FR-16", "FR-19", "FR-21", "CR-9", "INV-42", "INV-49", "INV-61", "SEC-2", "SEC-3"],
            scope: CaseScope::Backend,
            asserts: "assert_context_conformance",
        },
        ContractCase {
            name: "integrity",
            covers: &["FR-39", "CR-13", "INV-44"],
            scope: CaseScope::OptionalPort,
            asserts: "assert_integrity_conformance",
        },
        ContractCase {
            name: "global-claim",
            covers: &["FR-4", "CR-1"],
            scope: CaseScope::Backend,
            asserts: "assert_global_claim",
        },
        ContractCase {
            name: "output-provenance",
            covers: &["FR-20", "CR-3", "CR-7", "INV-6", "INV-48"],
            scope: CaseScope::Backend,
            asserts: "assert_output_provenance",
        },
        ContractCase {
            name: "durability-order",
            covers: &["CR-6", "INV-34", "INV-46"],
            scope: CaseScope::Backend,
            asserts: "assert_durability_order",
        },
        ContractCase {
            name: "concurrency",
            covers: &["CR-1", "CR-2", "CR-5", "INV-1", "INV-2", "INV-11"],
            scope: CaseScope::Backend,
            asserts: "assert_concurrency_conformance",
        },
        ContractCase {
            name: "chain-closure",
            covers: &["FR-28", "CR-12", "INV-47"],
            scope: CaseScope::Protocol,
            asserts: "assert_output_items_are_valid_input",
        },
        ContractCase {
            name: "protocol-subset",
            covers: &["FR-24", "FR-25", "FR-26", "INV-50", "INV-52", "SEC-6", "SEC-7"],
            scope: CaseScope::Protocol,
            asserts: "assert_protocol_subset_rejects",
        },
        ContractCase {
            name: "event-coalesce",
            // FR-11 ("the cursor is the only connection state") was claimed here
            // and removed — coalescing is about payload size, not stickiness.
            covers: &["INV-16"],
            scope: CaseScope::Protocol,
            asserts: "assert_event_coalescing",
        },
        ContractCase {
            name: "conversation",
            // FR-41 is *not* claimed here: this case can show the tail moves, but
            // "the next generation inherits it" is assembled in the service layer
            // and is covered by the L2 http scenarios.
            covers: &["FR-40", "FR-42", "FR-21", "INV-54", "INV-55", "SEC-2"],
            scope: CaseScope::Backend,
            asserts: "assert_conversation_conformance",
        },
        ContractCase {
            name: "conversation-events",
            covers: &[
                "FR-43", "FR-44", "CR-14", "CR-15", "CR-16", "INV-56", "INV-57", "INV-58",
                "INV-59", "SEC-2",
            ],
            scope: CaseScope::Backend,
            asserts: "assert_conversation_events_conformance",
        },
    ]
}

/// Outcome of one case, so callers can distinguish "passed" from "not run".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaseOutcome {
    Passed,
    /// An optional port was absent. Never silent: surfaced in the report.
    Skipped(&'static str),
}

/// Result of a whole suite run.
#[derive(Debug, Clone, Default)]
pub struct SuiteReport {
    pub label: String,
    pub passed: Vec<&'static str>,
    pub skipped: Vec<(&'static str, &'static str)>,
}

impl SuiteReport {
    /// Requirement ids substantiated by the cases that actually ran.
    ///
    /// Derived from the run, not from a literal list, so deleting an assertion
    /// lowers reported coverage instead of leaving it untouched.
    pub fn covered(&self) -> Vec<&'static str> {
        let mut out: Vec<&'static str> = cases()
            .iter()
            .filter(|c| self.passed.contains(&c.name))
            .flat_map(|c| c.covers.iter().copied())
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }
}

/// Dispatch one case by name.
///
/// Panics on an unknown name rather than skipping it: a case listed in
/// [`cases()`] but not dispatched here would otherwise be counted as covered
/// while never executing.
async fn run_case(ports: &PortSet, case: &ContractCase) -> CaseOutcome {
    match case.name {
        "event-log" => {
            assert_event_log_conformance(ports.event_log.clone(), &ports.node_tag).await
        }
        "ledger" => assert_ledger_conformance(ports).await,
        "cancel" => assert_cancel_conformance(ports).await,

        "context-chain" => assert_context_conformance(ports).await,
        "integrity" => match &ports.integrity {
            Some(integrity) => assert_integrity_conformance(integrity.clone()).await,
            None => return CaseOutcome::Skipped("backend supplies no ContentIntegrity port"),
        },
        "global-claim" => assert_global_claim(ports).await,
        "output-provenance" => assert_output_provenance(ports).await,
        "durability-order" => assert_durability_order(ports).await,
        "concurrency" => assert_concurrency_conformance(ports).await,
        "chain-closure" => assert_output_items_are_valid_input(),
        "protocol-subset" => assert_protocol_subset_rejects(),
        "event-coalesce" => assert_event_coalescing(),
        "conversation" => assert_conversation_conformance(ports).await,
        "conversation-events" => assert_conversation_events_conformance(ports).await,
        unknown => panic!(
            "contract case `{unknown}` is listed in cases() but has no dispatch arm; \
             it would be reported as covered without ever running"
        ),
    }
    CaseOutcome::Passed
}

/// Run the whole contract against one backend.
pub async fn run_suite(ports: &PortSet) -> SuiteReport {
    run_suite_inner(ports, "unlabelled", false).await
}

/// Same as [`run_suite`], announcing each case (used by `just verify l0`).
pub async fn run_suite_reported(ports: &PortSet, label: &str) -> SuiteReport {
    run_suite_inner(ports, label, true).await
}

async fn run_suite_inner(ports: &PortSet, label: &str, announce: bool) -> SuiteReport {
    let mut report = SuiteReport {
        label: label.to_string(),
        ..Default::default()
    };
    if announce {
        eprintln!("  [{label}]");
    }
    for case in cases() {
        if announce {
            eprint!("    {} ... ", case.name);
        }
        match run_case(ports, case).await {
            CaseOutcome::Passed => {
                if announce {
                    eprintln!("ok");
                }
                report.passed.push(case.name);
            }
            CaseOutcome::Skipped(reason) => {
                // Visible, because a silently skipped case is indistinguishable
                // from a passing one in the output.
                if announce {
                    eprintln!("SKIPPED ({reason})");
                }
                report.skipped.push((case.name, reason));
            }
        }
    }
    report
}

/// Build a [`PortSet`] over the in-memory adapters.
pub fn mem_ports() -> PortSet {
    let world = mock_server::MemWorld::new();
    PortSet {
        // One backend, mounted on both halves of the split port.
        intake: world.ledger.clone(),
        claims: world.ledger.clone(),
        event_log: world.event_log.clone(),
        conversation: world.conversation.clone(),
        integrity: world.integrity.clone(),
        node_tag: NodeTag::parse("node-a").expect("static tag"),
    }
}

pub async fn run_mem_suite() -> SuiteReport {
    run_suite(&mem_ports()).await
}

pub async fn run_mem_suite_reported() -> SuiteReport {
    run_suite_reported(&mem_ports(), "mem").await
}

#[cfg(test)]
mod tests {
    use super::*;
    use nova_responses::ResponseEvent;

    #[tokio::test]
    async fn mem_backend_satisfies_the_contract() {
        let report = run_mem_suite().await;
        assert!(
            report.skipped.is_empty(),
            "mem supplies every port: {report:?}"
        );
        assert_eq!(report.passed.len(), cases().len());
    }

    #[tokio::test]
    async fn contract_is_backend_agnostic() {
        // Two independently constructed backends must both pass, which is what
        // makes the suite reusable for the sql adapter at L3.
        run_suite(&mem_ports()).await;
        run_suite(&mem_ports()).await;
    }

    #[tokio::test]
    async fn every_listed_case_is_dispatched() {
        // Guards the split between the metadata table and the dispatcher: a case
        // present in cases() but missing an arm would be counted as covered
        // without running. run_case panics on an unknown name, so completing
        // this loop proves every name resolves.
        let ports = mem_ports();
        for case in cases() {
            let outcome = run_case(&ports, case).await;
            assert_eq!(outcome, CaseOutcome::Passed, "{} did not run", case.name);
        }
    }

    /// `covers` is a self-declaration, and the coverage figure is derived from it.
    /// This test is what makes the declaration falsifiable: every requirement id a
    /// case claims must appear inside the body of the function that supposedly
    /// substantiates it — in a doc comment, an inline comment, or an assertion
    /// message.
    ///
    /// Without this, a case could claim any id and the gate would happily count it.
    /// That is not a hypothetical: the first version of this table claimed FR-11
    /// ("no connection stickiness", an ingress property) for the event-log port
    /// case, and INV-16 (event coalescing) for it as well — neither of which that
    /// function can observe.
    #[test]
    fn covers_claims_are_substantiated_in_the_named_function() {
        let src = include_str!("lib.rs");
        let mut problems: Vec<String> = Vec::new();

        for case in cases() {
            let body = function_body(src, case.asserts)
                .unwrap_or_else(|| panic!("cannot locate `{}` in lib.rs", case.asserts));

            let unmentioned: Vec<&str> = case
                .covers
                .iter()
                .copied()
                .filter(|id| !body.contains(id))
                .collect();

            if !unmentioned.is_empty() {
                problems.push(format!(
                    "  {} ({}) claims {unmentioned:?}",
                    case.name, case.asserts
                ));
            }
        }
        assert!(
            problems.is_empty(),
            "unsubstantiated coverage claims:\n{}\nEither assert the requirement \
             in the named function and say so, or drop the claim — the coverage \
             gate counts these ids as verified.",
            problems.join("\n")
        );
    }

    /// Extract a top-level function body, doc comment included.
    fn function_body<'a>(src: &'a str, name: &str) -> Option<&'a str> {
        // Search for the signature, then walk back over the preceding doc comment
        // block so `/// INV-11: ...` counts as substantiation.
        let sig = src
            .find(&format!("pub async fn {name}("))
            .or_else(|| src.find(&format!("pub fn {name}(")))?;

        let mut start = src[..sig].rfind("\n\n").map(|i| i + 2).unwrap_or(0);
        if start > sig {
            start = sig;
        }
        // The body ends at the next top-level item.
        let rest = &src[sig..];
        let end = rest
            .find("\n// ---")
            .or_else(|| rest.find("\npub async fn "))
            .or_else(|| rest.find("\npub fn "))
            .map(|i| sig + i)
            .unwrap_or(src.len());
        Some(&src[start..end])
    }

    #[test]
    fn case_names_are_unique() {
        // Duplicate names would make the dispatcher ambiguous and inflate the
        // reported case count.
        let mut names: Vec<_> = cases().iter().map(|c| c.name).collect();
        let total = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), total, "duplicate contract case name");
    }

    #[test]
    fn every_case_substantiates_at_least_one_requirement() {
        // A case covering nothing cannot lower the coverage figure when removed,
        // which defeats the purpose of deriving coverage from the run.
        for case in cases() {
            assert!(
                !case.covers.is_empty(),
                "case `{}` lists no requirement ids",
                case.name
            );
        }
    }

    #[tokio::test]
    async fn optional_port_absence_is_reported_not_hidden() {
        // A backend without the integrity port must produce a visible skip, so
        // "no integrity implementation" can never read as "integrity verified".
        let mut ports = mem_ports();
        ports.integrity = None;
        let report = run_suite(&ports).await;
        assert_eq!(report.skipped.len(), 1);
        assert_eq!(report.skipped[0].0, "integrity");
        assert!(
            !report.covered().contains(&"CR-13"),
            "a skipped case must not contribute coverage"
        );
    }

    /// An event log whose sequence allocation is read-then-increment with a yield
    /// in between — the classic mistake this contract exists to catch.
    ///
    /// Used to verify the *checker*, not the product. A concurrency assertion that
    /// cannot fail is worse than no assertion: it reports safety it never
    /// established. Without this, `assert_concurrency_conformance` passing would
    /// only prove the mem adapter happens not to interleave.
    struct RaceyEventLog {
        next: std::sync::Arc<std::sync::atomic::AtomicU64>,
        events: std::sync::Arc<std::sync::Mutex<Vec<ResponseEvent>>>,
    }

    #[async_trait::async_trait]
    impl ResponseEventLog for RaceyEventLog {
        async fn append(&self, event: AppendEvent) -> Result<u64, EventLogError> {
            use std::sync::atomic::Ordering;
            // Read...
            let seen = self.next.load(Ordering::SeqCst);
            // ...let another task in...
            tokio::task::yield_now().await;
            // ...then write. Two tasks can observe the same value.
            self.next.store(seen + 1, Ordering::SeqCst);
            self.events.lock().expect("lock").push(event.with_seq(seen));
            Ok(seen)
        }

        async fn read_after(
            &self,
            _response_id: &ResponseId,
            starting_after: Option<u64>,
            limit: usize,
            _wait: Duration,
        ) -> Result<Vec<ResponseEvent>, EventLogError> {
            let g = self.events.lock().expect("lock");
            Ok(g.iter()
                .filter(|e| match starting_after {
                    Some(after) => e.sequence_number() > after,
                    None => true,
                })
                .take(limit)
                .cloned()
                .collect())
        }

        async fn close(&self, _response_id: &ResponseId, _now_ms: u64, _retain: Duration)
            -> Result<(), EventLogError> {
            Ok(())
        }

        async fn sweep_expired(&self, _now_ms: u64) -> Result<u64, EventLogError> {
            Ok(0)
        }

        async fn remove(&self, _response_id: &ResponseId) -> Result<(), EventLogError> {
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_concurrency_check_rejects_a_racey_sequence_allocator() {
        let mut ports = mem_ports();
        ports.event_log = Arc::new(RaceyEventLog {
            next: Default::default(),
            events: Default::default(),
        });

        // Run in a task so the assertion failure surfaces as a join error rather
        // than aborting this test.
        let handle = tokio::spawn(async move {
            assert_concurrency_conformance(&ports).await;
        });
        let result = handle.await;
        assert!(
            result.is_err(),
            "the concurrency contract accepted a read-then-increment sequence \
             allocator; it therefore proves nothing about atomicity and would pass \
             for an implementation that silently drops events on resume"
        );
    }

    #[tokio::test]
    async fn coverage_is_derived_from_the_run() {
        let report = run_mem_suite().await;
        let covered = report.covered();
        for expected in ["CR-1", "CR-9", "CR-12", "CR-13", "INV-11", "INV-40"] {
            assert!(
                covered.contains(&expected),
                "{expected} missing from {covered:?}"
            );
        }
    }
}
