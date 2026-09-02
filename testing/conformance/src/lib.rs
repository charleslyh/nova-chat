//! L0 port contract.
//!
//! **The same assertions run against every backend.** That is the acceptance
//! criterion for the ports being real abstractions rather than descriptions of
//! the in-memory implementation: `run_suite` takes trait objects, and both the
//! mem and sql adapters are fed through it unchanged.
//!
//! Each case uses a freshly generated tenant so the suite is safe to run
//! repeatedly against a persistent backend without cleanup between passes.

use std::sync::Arc;

use nova_responses_core::protocol::{CreateResponseRequest, InputLimits, ResponseItem};
use nova_responses_core::{
    canonical_items, AgentId, Attempt, ChainLimits, ContentIntegrity, ContextError, ContextStore,
    CreateOutcome, EventBody, EventLogError, IdempotencyKey, LedgerError, NodeTag, ResponseEvent,
    ResponseEventKind, ResponseEventLog, ResponseId, ResponseLedger, ResponseStatus, StoredResponse,
    TenantId, Usage,
};

/// The set of ports under test. Backend-agnostic by construction.
#[derive(Clone)]
pub struct PortSet {
    pub ledger: Arc<dyn ResponseLedger>,
    pub event_log: Arc<dyn ResponseEventLog>,
    pub context: Arc<dyn ContextStore>,
    pub integrity: Option<Arc<dyn ContentIntegrity>>,
    pub node_tag: NodeTag,
}

impl PortSet {
    fn new_id(&self) -> ResponseId {
        ResponseId::new(self.node_tag.clone())
    }
}

/// Unique tenant per case, so a persistent backend needs no truncation between
/// runs and cases cannot interfere with one another.
fn fresh_tenant(prefix: &str) -> TenantId {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    TenantId::parse(&format!("{prefix}-{}", &suffix[..12])).expect("generated tenant is valid")
}

fn fresh_key() -> IdempotencyKey {
    IdempotencyKey(uuid::Uuid::new_v4().to_string())
}

fn record(
    ports: &PortSet,
    id: &ResponseId,
    previous: Option<&ResponseId>,
    tenant: &TenantId,
    stored: bool,
    status: ResponseStatus,
) -> StoredResponse {
    StoredResponse {
        response_id: id.clone(),
        previous_response_id: previous.cloned(),
        tenant_id: tenant.clone(),
        model: "test-model".into(),
        instructions: Some("INSTRUCTIONS-MARKER".into()),
        input_items: vec![ResponseItem::user_text(format!("in-{}", id.uuid()))],
        output_items: vec![],
        status,
        usage: Usage::default(),
        created_at_ms: 1_000,
        completed_at_ms: None,
        stored,
        expires_at_ms: None,
        integrity: None,
        integrity_alg: None,
        node_tag: ports.node_tag.clone(),
        idempotency_key: None,
        owner: None,
        attempt: Attempt::default(),
        context: Vec::new(),
        context_depth: 0,
    }
}

fn event(id: &ResponseId, kind: ResponseEventKind, payload: &str) -> ResponseEvent {
    let body = if payload.is_empty() {
        EventBody::Empty {}
    } else {
        EventBody::Delta {
            item_id: String::new(),
            output_index: 0,
            content_index: None,
            delta: payload.to_string(),
        }
    };
    ResponseEvent {
        response_id: id.clone(),
        sequence_number: 0,
        kind,
        attempt: None,
        body,
    }
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

    let all = log.read_after(&id, None, 100, 0).await.expect("read all");
    let seqs: Vec<u64> = all.iter().map(|e| e.sequence_number).collect();
    assert_eq!(seqs, vec![0, 1, 2, 3, 4], "no gaps and no repeats");

    // `Some(0)` must skip event 0; `None` must include it. If these collapsed,
    // resuming from the very first event would be impossible.
    let after_zero = log.read_after(&id, Some(0), 100, 0).await.expect("read");
    assert_eq!(
        after_zero.first().map(|e| e.sequence_number),
        Some(1),
        "starting_after is exclusive"
    );
    let from_start = log.read_after(&id, None, 100, 0).await.expect("read");
    assert_eq!(from_start.first().map(|e| e.sequence_number), Some(0));

    // Beyond the tip: empty, not an error — the response may still be running.
    let future = log.read_after(&id, Some(999), 10, 0).await.expect("read");
    assert!(future.is_empty());

    // Unknown ids are reported, never treated as an empty stream.
    let unknown = ResponseId::new(node_tag.clone());
    assert_eq!(
        log.read_after(&unknown, None, 10, 0).await,
        Err(EventLogError::Unknown)
    );

    // After the retention window: explicit expiry, and crucially **no partial
    // data and no fallback layer** (INV-40).
    log.close(&id, 10_000, 1_000).await.expect("close");
    log.sweep_expired(11_001).await.expect("sweep");
    assert_eq!(
        log.read_after(&id, None, 10, 0).await,
        Err(EventLogError::Expired)
    );
    assert_eq!(
        log.read_after(&id, Some(2), 10, 0).await,
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
        .ledger
        .create(
            record(ports, &id, None, &tenant, true, ResponseStatus::Queued),
            key.clone(),
            1_000,
        )
        .await
        .expect("create");
    assert_eq!(outcome, CreateOutcome::Accepted { response_id: id.clone() });

    // Replay far in the future still returns the original: the gate has no TTL
    // window, so a late retry cannot produce a second response (INV-2).
    let replay = ports
        .ledger
        .create(
            record(ports, &ports.new_id(), None, &tenant, true, ResponseStatus::Queued),
            key,
            u64::MAX,
        )
        .await
        .expect("replay");
    assert_eq!(replay, CreateOutcome::Duplicate { response_id: id.clone() });

    let agent = AgentId::new();
    let claimed = ports
        .ledger
        .claim(&ports.node_tag, agent, 2_000, 60_000)
        .await
        .expect("claim")
        .expect("something claimable");
    assert_eq!(claimed.attempt, Attempt(1), "attempt starts at 1 and increments");
    let claimed_id = claimed.record.response_id.clone();

    // The fence accepts the current attempt and rejects anything else.
    ports
        .ledger
        .check_attempt(&claimed_id, claimed.attempt)
        .await
        .expect("current attempt is valid");
    assert_eq!(
        ports
            .ledger
            .check_attempt(&claimed_id, Attempt(claimed.attempt.0 + 1))
            .await,
        Err(LedgerError::StaleAttempt)
    );

    ports
        .ledger
        .complete(
            &claimed_id,
            claimed.attempt,
            ResponseStatus::Completed,
            Usage::new(3, 4),
            3_000,
        )
        .await
        .expect("complete");

    // Completing twice must fail: the second call is either a duplicate delivery
    // or a superseded holder.
    assert!(ports
        .ledger
        .complete(
            &claimed_id,
            claimed.attempt,
            ResponseStatus::Completed,
            Usage::default(),
            3_100,
        )
        .await
        .is_err());

    // A non-terminal target status is a programming error, not a transition.
    assert!(matches!(
        ports
            .ledger
            .complete(
                &claimed_id,
                claimed.attempt,
                ResponseStatus::InProgress,
                Usage::default(),
                3_200,
            )
            .await,
        Err(LedgerError::InvalidTransition(_))
    ));

    let fetched = ports
        .ledger
        .get(&claimed_id)
        .await
        .expect("get")
        .expect("present");
    assert_eq!(fetched.status, ResponseStatus::Completed);
    assert_eq!(fetched.usage.total_tokens, 7);
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
        .ledger
        .create(
            record(ports, &id, None, &tenant, true, ResponseStatus::Queued),
            fresh_key(),
            1_000,
        )
        .await
        .expect("create");

    // Foreign tenants are told "not found", never "forbidden" (SEC-2).
    assert_eq!(
        ports
            .ledger
            .cancel(&fresh_tenant("intruder"), &id, 1_500)
            .await,
        Err(LedgerError::NotFound)
    );

    ports
        .ledger
        .record_partial_usage(&id, Attempt(1), Usage::new(9, 1))
        .await
        .expect("record partial usage");

    // Note what cannot be asserted here, and why.
    //
    // `record_partial_usage` files the amount under `(response_id, attempt)` in a
    // side table — deliberately, since the record's own `usage` belongs to the
    // attempt that completes. But `ResponseLedger` exposes **no method to read
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
                .ledger
                .record_partial_usage(&missing, Attempt(1), Usage::new(1, 1))
                .await,
            Err(LedgerError::NotFound)
        ),
        "booking usage against an unknown response must fail rather than create a \
         dangling charge"
    );
    ports.ledger.cancel(&tenant, &id, 2_000).await.expect("cancel");

    let after = ports.ledger.get(&id).await.expect("get").expect("present");
    assert_eq!(after.status, ResponseStatus::Cancelled);
    assert!(after.status.is_terminal());

    // Cancelling twice is a conflict, not a silent no-op.
    assert!(matches!(
        ports.ledger.cancel(&tenant, &id, 2_100).await,
        Err(LedgerError::InvalidTransition(_))
    ));
}

/// Orphan reclamation contract.
///
/// - **FR-5**: a response whose executor is gone becomes reclaimable.
/// - **FR-38 / CR-6**: reclaimed work fails explicitly instead of lingering
///   non-terminal forever, so an accepted response always reaches an end state.
/// - **FR-36 / INV-45**: reclaim is scoped to the reclaiming node; a peer's
///   in-flight work is untouched.
pub async fn assert_orphan_reclaim_conformance(ports: &PortSet) {
    let tenant = fresh_tenant("orphan");
    let id = ports.new_id();
    ports
        .ledger
        .create(
            record(ports, &id, None, &tenant, true, ResponseStatus::Queued),
            fresh_key(),
            1_000,
        )
        .await
        .expect("create");

    let reclaimed = ports
        .ledger
        .reclaim_orphans(&ports.node_tag, 5_000)
        .await
        .expect("reclaim");
    assert!(
        reclaimed.iter().any(|c| c.response_id == id),
        "this node's queued work must be reclaimed"
    );

    let after = ports.ledger.get(&id).await.expect("get").expect("present");
    assert_eq!(after.status, ResponseStatus::Failed);
    assert!(
        after.attempt > Attempt::default(),
        "the fence must be raised so a dead holder cannot append"
    );

    // A different node's work is untouched.
    let other_tag = NodeTag::parse("other-node").expect("tag");
    let other_tag_for_cleanup = other_tag.clone();
    let theirs = ResponseId::new(other_tag.clone());
    let mut theirs_rec = record(ports, &theirs, None, &tenant, true, ResponseStatus::Queued);
    theirs_rec.node_tag = other_tag;
    ports
        .ledger
        .create(theirs_rec, fresh_key(), 1_000)
        .await
        .expect("create");
    ports
        .ledger
        .reclaim_orphans(&ports.node_tag, 6_000)
        .await
        .expect("reclaim");
    let untouched = ports.ledger.get(&theirs).await.expect("get").expect("present");
    assert_eq!(
        untouched.status,
        ResponseStatus::Queued,
        "reclaim must be scoped to the calling node"
    );

    // Release it before returning.
    //
    // Now that claiming is node-scoped (FR-4), a later case cannot drain another
    // node's queue — so a record left queued here would permanently occupy part of
    // the process-wide admission budget, and the overload case would measure this
    // case's residue instead of its own work.
    ports
        .ledger
        .reclaim_orphans(&other_tag_for_cleanup, 7_000)
        .await
        .expect("release the other node's queued work");
}

// ------------------------------------------------------------------ context

/// Context store and chain resolution contract.
///
/// - **FR-15**: the storage switch is honoured in both positions.
/// - **FR-16 / CR-9**: history is reassembled from `previous_response_id`,
///   deterministically and in chronological order.
/// - **FR-17 / INV-41 / INV-42**: depth and byte ceilings yield diagnosable
///   errors — never a silent truncation.
/// - **FR-18 / CR-10 / INV-43**: all four break kinds are distinguishable, so a
///   broken chain cannot degrade quietly into a single turn.
/// - **FR-19 / INV-49**: instructions do not cross turns.
/// - **FR-21**: single-response deletion and tenant-wide purge.
/// - **FR-22**: expired content is swept.
/// - **SEC-2 / SEC-3**: tenancy is checked per link; a foreign anchor reads as
///   absent rather than forbidden.
/// that never includes instructions and never truncates silently.
pub async fn assert_context_conformance(ports: &PortSet) {
    let tenant = fresh_tenant("ctx");
    let store = &ports.context;

    // Round trip.
    let solo = ports.new_id();
    store
        .put(record(ports, &solo, None, &tenant, true, ResponseStatus::Completed))
        .await
        .expect("put");
    let got = store
        .get(&tenant, &solo)
        .await
        .expect("get")
        .expect("present");
    assert_eq!(got.response_id, solo);

    // Foreign tenants see nothing.
    assert!(store
        .get(&fresh_tenant("other"), &solo)
        .await
        .expect("get")
        .is_none());

    // Output is committed by the execution side, not derived from events.
    store
        .append_output(
            &tenant,
            &solo,
            vec![ResponseItem::assistant_text("answer")],
            Usage::new(1, 2),
            ResponseStatus::Completed,
            2_000,
        )
        .await
        .expect("append output");
    let with_output = store.get(&tenant, &solo).await.unwrap().unwrap();
    assert_eq!(with_output.output_items.len(), 1);
    assert_eq!(with_output.usage.total_tokens, 3);

    // Build a three-link chain, materialising history the way the gateway does at
    // create time (D24): each link snapshots everything before it as a flat copy.
    let mut ids = vec![solo.clone()];
    let mut history: Vec<ResponseItem> = {
        let solo_rec = store.get(&tenant, &solo).await.unwrap().unwrap();
        solo_rec.chain_items().cloned().collect::<Vec<_>>()
    };
    for _ in 0..2 {
        let id = ports.new_id();
        let mut rec = record(
            ports,
            &id,
            ids.last(),
            &tenant,
            true,
            ResponseStatus::Completed,
        );
        rec.output_items = vec![ResponseItem::assistant_text("a")];
        rec.context = history.clone();
        // `ids` still holds the ancestors built so far (including `solo`), so its
        // length is exactly how many turns this link inherits.
        rec.context_depth = ids.len();
        let own: Vec<ResponseItem> = rec.chain_items().cloned().collect();
        store.put(rec).await.expect("put");
        history.extend(own);
        ids.push(id);
    }

    let resolved = store
        .resolve_chain(&tenant, ids.last().unwrap(), ChainLimits::default())
        .await
        .expect("resolve");
    assert_eq!(resolved.depth, 3);
    assert!(resolved.bytes > 0);
    assert_eq!(
        resolved.items.len(),
        6,
        "each link contributes its input and its output"
    );

    // Chronological order: the oldest link's input comes first.
    let encoded_first = canonical_items(&resolved.items[..1]);
    assert!(
        encoded_first.contains(&format!("in-{}", ids[0].uuid())),
        "chain must be returned oldest-first, got {encoded_first}"
    );

    // Instructions must never cross a turn boundary (INV-49).
    let encoded_all = canonical_items(&resolved.items);
    assert!(
        !encoded_all.contains("INSTRUCTIONS-MARKER"),
        "instructions leaked into chain output"
    );

    // Bounds are errors, never truncations (INV-41).
    assert_eq!(
        store
            .resolve_chain(
                &tenant,
                ids.last().unwrap(),
                ChainLimits {
                    max_depth: 2,
                    ..ChainLimits::default()
                },
            )
            .await,
        Err(ContextError::ChainTooLong { limit: 2 })
    );
    assert_eq!(
        store
            .resolve_chain(
                &tenant,
                ids.last().unwrap(),
                ChainLimits {
                    max_bytes: 4,
                    ..ChainLimits::default()
                },
            )
            .await,
        Err(ContextError::ChainTooLarge { limit: 4 })
    );

    // A foreign anchor is indistinguishable from a missing one: reporting them
    // differently would confirm that an id exists (SEC-2).
    let foreign_tenant = fresh_tenant("foreign");
    let theirs = ports.new_id();
    store
        .put(record(
            ports,
            &theirs,
            None,
            &foreign_tenant,
            true,
            ResponseStatus::Completed,
        ))
        .await
        .expect("put");
    assert!(matches!(
        store
            .resolve_chain(&tenant, &theirs, ChainLimits::default())
            .await,
        Err(ContextError::ChainBroken(_))
    ));
    let ghost = ports.new_id();
    assert!(matches!(
        store
            .resolve_chain(&tenant, &ghost, ChainLimits::default())
            .await,
        Err(ContextError::ChainBroken(_))
    ));

    // Cross-tenant detection moved to create time (D24): a snapshot is built by
    // resolving the `previous` response's own snapshot, and that resolution already
    // checks the tenant — so a foreign link can never be materialised, and
    // resolution no longer walks far enough to observe one.

    // An unstored anchor cannot be resolved (FR-18).
    let unstored = ports.new_id();
    store
        .put(record(
            ports,
            &unstored,
            None,
            &tenant,
            false,
            ResponseStatus::Completed,
        ))
        .await
        .expect("put");
    assert_eq!(
        store
            .resolve_chain(&tenant, &unstored, ChainLimits::default())
            .await,
        Err(ContextError::NotStored)
    );

    // Deletion is surgical (D24): the deleted link is stripped from every
    // downstream snapshot, and descendants themselves survive — the property the
    // snapshot exists to provide.
    assert!(store.delete(&tenant, &solo).await.expect("delete"));
    assert!(!store
        .delete(&tenant, &solo)
        .await
        .expect("second delete is a no-op"));

    // Resolving the deleted link itself is a broken anchor.
    assert!(matches!(
        store
            .resolve_chain(&tenant, &solo, ChainLimits::default())
            .await,
        Err(ContextError::ChainBroken(_))
    ));

    // A descendant still resolves, with the full inherited history intact —
    // deletion is record-level, not content-level (D24): "remove from the
    // conversation" removes the record, the flat copy it contributed lives on.
    let after_delete = store
        .resolve_chain(&tenant, ids.last().unwrap(), ChainLimits::default())
        .await
        .expect("descendant must survive the deletion");
    assert_eq!(
        after_delete.depth, 3,
        "the snapshot keeps every link it inherited, including the deleted head"
    );
    assert_eq!(after_delete.items.len(), 6);
    let encoded_after = canonical_items(&after_delete.items);
    assert!(
        encoded_after.contains(&format!("in-{}", solo.uuid())),
        "the deleted link's content must remain in the descendant's snapshot: {encoded_after}"
    );

    // Expiry sweep only removes what is due.
    let expiring = ports.new_id();
    let mut rec = record(ports, &expiring, None, &tenant, true, ResponseStatus::Completed);
    rec.expires_at_ms = Some(5_000);
    store.put(rec).await.expect("put");
    store.sweep_expired(4_999, 100).await.expect("sweep early");
    assert!(store.get(&tenant, &expiring).await.unwrap().is_some());
    store.sweep_expired(5_000, 100).await.expect("sweep due");
    assert!(store.get(&tenant, &expiring).await.unwrap().is_none());

    // Bulk purge is tenant-scoped.
    let purged = store.delete_by_tenant(&tenant).await.expect("purge");
    assert!(purged > 0);
    assert!(store
        .get(&foreign_tenant, &theirs)
        .await
        .unwrap()
        .is_some(), "purge must not touch other tenants");

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
        inline.validate(&InputLimits::default()).is_err(),
        "inline binary must be rejected"
    );

    let internal: CreateResponseRequest = serde_json::from_str(
        r#"{"model":"m","input":[{"type":"message","role":"user","content":[{"type":"input_image","image_url":"https://169.254.169.254/x"}]}]}"#,
    )
    .expect("structurally valid");
    assert!(
        internal.validate(&InputLimits::default()).is_err(),
        "internal addresses must be rejected"
    );

    // And the supported shapes must actually work, otherwise "strict" would just
    // mean "broken".
    let ok: CreateResponseRequest =
        serde_json::from_str(r#"{"model":"m","input":"hi","store":true,"temperature":0.5}"#)
            .expect("supported request");
    assert!(ok.validate(&InputLimits::default()).is_ok());
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

/// Reconnect backoff.
///
/// **FR-35 / INV-33**: delays grow with attempts and carry jitter, so a fleet that
/// loses its upstream does not resynchronise into a thundering herd.
pub fn assert_reconnect_backoff() {
    use nova_responses_core::JitteredBackoff;
    let b = JitteredBackoff::default();
    assert!(b.delay(3, 1.0) > b.delay(0, 1.0));
    let (lo, hi) = b.delay_bounds(2);
    assert!(hi > lo, "jitter spread required");
    assert!(hi.as_millis() <= 30_000 * 2);
}

// ------------------------------------------------------------- claim locality

/// FR-4 / D23: a response is only ever executed by the node that created it.
///
/// This is the assertion whose absence allowed a real defect to ship. With the
/// in-memory backend each node holds its own ledger, so the constraint held
/// automatically and nothing expressed it. Once D21 made the ledger shared, the
/// selection had no node predicate — and node-b's work could be handed to node-a.
///
/// The consequence is invisible rather than loud: increments land in node-a's
/// in-flight buffer, while a subscriber routes by the node tag inside the id and
/// is sent to node-b. It sees `Created` and then nothing, forever, with no error
/// raised anywhere. Indistinguishable from a model that produced no output.
pub async fn assert_claim_locality(ports: &PortSet) {
    let tenant = fresh_tenant("locality");

    // Drain this node's queue so the assertion is about what follows.
    loop {
        let Some(c) = ports
            .ledger
            .claim(&ports.node_tag, AgentId(uuid::Uuid::new_v4()), 1_000, 30_000)
            .await
            .expect("drain claim")
        else {
            break;
        };
        ports
            .ledger
            .complete(
                &c.record.response_id,
                c.attempt,
                ResponseStatus::Completed,
                Usage::default(),
                1_000,
            )
            .await
            .expect("drain complete");
    }

    // A response owned by a *different* node.
    let other_node = NodeTag::parse("node-zz").expect("static tag");
    let foreign_id = ResponseId::new(other_node.clone());
    let mut foreign = record(ports, &foreign_id, None, &tenant, true, ResponseStatus::Queued);
    foreign.node_tag = other_node.clone();
    ports
        .ledger
        .create(foreign, fresh_key(), 1_000)
        .await
        .expect("create foreign");

    assert!(
        ports
            .ledger
            .claim(&ports.node_tag, AgentId(uuid::Uuid::new_v4()), 1_100, 30_000)
            .await
            .expect("claim")
            .is_none(),
        "this node claimed a response belonging to `{}`. Its increments would go to \
         this node's in-flight buffer while subscribers are routed to the owning \
         node — they would see the created event and then silence (FR-4).",
        other_node.as_str()
    );

    // The foreign response must still be claimable by its owner: skipping it may
    // not consume it, or it would be stranded in the queue forever.
    let by_owner = ports
        .ledger
        .claim(&other_node, AgentId(uuid::Uuid::new_v4()), 1_200, 30_000)
        .await
        .expect("claim by owner")
        .expect("the owning node must still be able to claim its own work");
    assert_eq!(
        by_owner.record.response_id, foreign_id,
        "skipping another node's work must defer it, not discard it"
    );

    // And this node can still claim its own.
    let mine = ports.new_id();
    ports
        .ledger
        .create(
            record(ports, &mine, None, &tenant, true, ResponseStatus::Queued),
            fresh_key(),
            1_300,
        )
        .await
        .expect("create local");
    let claimed = ports
        .ledger
        .claim(&ports.node_tag, AgentId(uuid::Uuid::new_v4()), 1_400, 30_000)
        .await
        .expect("claim local")
        .expect("a node must be able to claim work it owns");
    assert_eq!(claimed.record.response_id, mine);
    assert_eq!(
        claimed.attempt.0, 1,
        "a first claim must produce attempt 1, otherwise the fence cannot tell a \
         retry from the original"
    );

    // Drive both claims to a terminal state before returning.
    //
    // Not tidiness: the admission counter is process-wide, so in-flight work left
    // behind here would silently consume another case's capacity budget. The
    // overload case then measures this case's leftovers instead of its own work.
    for c in [&by_owner, &claimed] {
        ports
            .ledger
            .complete(
                &c.record.response_id,
                c.attempt,
                ResponseStatus::Completed,
                Usage::default(),
                1_500,
            )
            .await
            .expect("release claimed work");
    }
}

// ------------------------------------------------------------ overload safety

/// CR-8: refusing work under pressure must not corrupt anything.
///
/// The existing overload scenarios establish only that a refusal *happens*: they
/// create one response, create a second, and observe `Overloaded`. But CR-8 is
/// about pressure — "no double claim, no lost response, no sequence fork **while
/// rejecting**" — and a serial pair of creates applies none. Rejection paths are
/// exactly where counters get decremented twice or a slot leaks, and none of that
/// is visible without contention.
///
/// So: hammer the admission boundary concurrently and check the three properties
/// CR-8 actually names.
///
/// - **FR-33**: the queued/in-flight ceiling is enforced, and a request over it is
///   refused rather than queued without bound.
/// - **INV-29**: the refusal is uniform — every racer receives a verdict, and no
///   request is silently dropped instead of being answered.
pub async fn assert_overload_integrity(ports: &PortSet) {
    const LIMIT: usize = 3;
    const RACERS: usize = 24;

    let restore = ports.ledger.pending_limit();
    let tenant = fresh_tenant("overload");

    // Clear anything left in flight by earlier cases, so the limit applies to this
    // case's work alone and the accepted count is attributable.
    //
    // Claiming is not enough: the admission counter tracks queued *and* in-progress
    // work, so a claim merely moves a record between two states that both consume a
    // slot. Each one has to be driven to a terminal state.
    loop {
        let Some(c) = ports
            .ledger
            .claim(&ports.node_tag, AgentId(uuid::Uuid::new_v4()), 1_000, 30_000)
            .await
            .expect("drain claim")
        else {
            break;
        };
        ports
            .ledger
            .complete(
                &c.record.response_id,
                c.attempt,
                ResponseStatus::Completed,
                Usage::default(),
                1_000,
            )
            .await
            .expect("drain complete");
    }

    ports.ledger.set_pending_limit(LIMIT);

    let mut handles = Vec::new();
    for _ in 0..RACERS {
        let ledger = ports.ledger.clone();
        let id = ports.new_id();
        let rec = record(ports, &id, None, &tenant, true, ResponseStatus::Queued);
        handles.push(tokio::spawn(async move {
            ledger.create(rec, fresh_key(), 2_000).await
        }));
    }

    let mut accepted = Vec::new();
    let mut refused = 0usize;
    for h in handles {
        match h.await.expect("create task panicked").expect("create call") {
            CreateOutcome::Accepted { response_id } => accepted.push(response_id),
            CreateOutcome::Overloaded => refused += 1,
            other => panic!("unexpected outcome at the admission boundary: {other:?}"),
        }
    }

    // 1. The ceiling holds exactly. Over-admitting means the guard is a
    //    check-then-act read; under-admitting means refusals leak slots, and the
    //    node quietly loses capacity it was configured to have.
    assert_eq!(
        accepted.len(),
        LIMIT,
        "expected exactly {LIMIT} admissions under contention, got {} accepted and \
         {refused} refused. Admitting more than the limit defeats the protection; \
         admitting fewer means a rejected create consumed a slot it never held.",
        accepted.len()
    );
    assert_eq!(accepted.len() + refused, RACERS, "every racer must get a verdict");

    // 2. No response is lost: everything reported accepted must be retrievable.
    //    A create that returns Accepted and leaves nothing behind is the failure
    //    CR-8 names as "lost response", and pressure is when it happens.
    for id in &accepted {
        assert!(
            ports
                .ledger
                .get(id)
                .await
                .expect("get")
                .is_some(),
            "response {id} was accepted while overloaded but cannot be read back"
        );
    }

    // 3. No double claim and no sequence fork among the survivors.
    let mut claim_handles = Vec::new();
    for _ in 0..RACERS {
        let ledger = ports.ledger.clone();
        let node = ports.node_tag.clone();
        claim_handles.push(tokio::spawn(async move {
            ledger
                .claim(&node, AgentId(uuid::Uuid::new_v4()), 2_100, 30_000)
                .await
        }));
    }
    let mut claimed: Vec<String> = Vec::new();
    for h in claim_handles {
        if let Some(c) = h.await.expect("claim task panicked").expect("claim call") {
            claimed.push(c.record.response_id.to_string());
        }
    }
    let distinct: std::collections::BTreeSet<_> = claimed.iter().cloned().collect();
    assert_eq!(
        distinct.len(),
        claimed.len(),
        "a response was claimed twice while the node was refusing work: {claimed:?}"
    );
    assert_eq!(
        distinct.len(),
        LIMIT,
        "every admitted response must be claimable exactly once; {} admitted but \
         {} claimable",
        LIMIT,
        distinct.len()
    );

    // Sequence allocation must remain sound for work admitted under pressure.
    if let Some(first) = accepted.first() {
        let mut seq_handles = Vec::new();
        for i in 0..8u64 {
            let log = ports.event_log.clone();
            let ev = event(first, ResponseEventKind::OutputTextDelta, &format!("p{i}"));
            seq_handles.push(tokio::spawn(async move { log.append(ev).await }));
        }
        let mut seqs = Vec::new();
        for h in seq_handles {
            seqs.push(h.await.expect("append task panicked").expect("append"));
        }
        seqs.sort_unstable();
        assert_eq!(
            seqs,
            (0..8u64).collect::<Vec<_>>(),
            "sequence numbers forked for a response admitted under pressure"
        );
    }

    ports.ledger.set_pending_limit(restore);
}

// -------------------------------------------------------- output provenance

/// FR-20 / INV-6 / CR-3 / CR-7: stored output comes from the executor's terminal
/// submission, never from replaying the event stream.
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
    ports
        .context
        .put(record(ports, &id, None, &tenant, true, ResponseStatus::InProgress))
        .await
        .expect("put");

    // The executor streams deltas for the caller's benefit...
    for (i, chunk) in ["Sta", "ble ", "answer"].iter().enumerate() {
        let mut ev = event(&id, ResponseEventKind::OutputTextDelta, chunk);
        ev.sequence_number = i as u64;
        ports.event_log.append(ev).await.expect("append delta");
    }

    // ...and separately submits the terminal items. Two write paths, deliberately.
    ports
        .context
        .append_output(
            &tenant,
            &id,
            vec![ResponseItem::assistant_text("Stable answer")],
            Usage::new(3, 4),
            ResponseStatus::Completed,
            2_000,
        )
        .await
        .expect("append_output");

    // Now discard the event stream entirely, as eviction or a node restart would.
    ports
        .event_log
        .close(&id, 3_000, 1_000)
        .await
        .expect("close");
    // Strictly past the retention window, matching the event-log case's convention.
    ports.event_log.sweep_expired(4_001).await.expect("sweep");
    assert!(
        matches!(
            ports.event_log.read_after(&id, None, 64, 0).await,
            Err(EventLogError::Expired)
        ),
        "the event stream must be genuinely gone for this check to mean anything"
    );

    // FR-20: the durable record is untouched by that loss.
    let stored = ports
        .context
        .get(&tenant, &id)
        .await
        .expect("context get")
        .expect("stored content must survive the loss of the event stream");
    assert_eq!(
        stored.status,
        ResponseStatus::Completed,
        "terminal status is recorded by the submission, not inferred from events"
    );
    assert!(
        canonical_items(&stored.output_items).contains("Stable answer"),
        "output items must come from the executor's terminal submission (FR-20); \
         if they were derived by replaying the event stream, discarding that \
         stream would have emptied them, making durable history depend on a \
         bounded transient buffer"
    );
    assert_eq!(
        stored.usage.total_tokens,
        7,
        "usage accompanies the terminal submission, not the delta events"
    );

    // CR-7 / INV-6: a superseded holder cannot inject events afterwards. Checked
    // here against the real append path rather than the advisory pre-check, since
    // it is the write that must be fenced — an implementation could pass a
    // `check_attempt` probe and still accept the write that follows it.
    let fenced = ports.new_id();
    ports
        .ledger
        .create(
            record(ports, &fenced, None, &tenant, true, ResponseStatus::Queued),
            fresh_key(),
            1_000,
        )
        .await
        .expect("create");
    let claimed = ports
        .ledger
        .claim(&ports.node_tag, AgentId(uuid::Uuid::new_v4()), 1_100, 30_000)
        .await
        .expect("claim")
        .expect("something was queued");
    let live = claimed.attempt;

    let mut ok_ev = event(&claimed.record.response_id, ResponseEventKind::OutputTextDelta, "live");
    ok_ev.attempt = Some(live);
    ports
        .event_log
        .append(ok_ev)
        .await
        .expect("the current holder must be able to write");

    let stale = Attempt(live.0.saturating_sub(1));
    let mut stale_ev = event(
        &claimed.record.response_id,
        ResponseEventKind::OutputTextDelta,
        "from a reaped holder",
    );
    stale_ev.attempt = Some(stale);
    let rejected = ports.event_log.append(stale_ev).await;
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
    let mut rec = record(ports, &id, None, &tenant, true, ResponseStatus::Queued);
    rec.input_items = vec![ResponseItem::user_text("durable-marker")];

    let outcome = ports
        .ledger
        .create(rec, fresh_key(), 1_000)
        .await
        .expect("create");
    assert!(matches!(outcome, CreateOutcome::Accepted { .. }));

    // No sleep, no retry loop: "eventually visible" is precisely what this
    // invariant forbids.
    let seen = ports
        .ledger
        .get(&id)
        .await
        .expect("ledger get")
        .expect("a response reported as accepted must be readable immediately");
    assert_eq!(seen.response_id, id);
    assert!(
        seen.stored,
        "the record must retain store=true, otherwise the chain it anchors cannot \
         be resolved later"
    );

    // And the content side must be visible too, since `store: true` was honoured
    // as part of the same acknowledgement.
    let stored = ports
        .context
        .get(&tenant, &id)
        .await
        .expect("context get")
        .expect(
            "content must be readable the instant create() succeeds; acknowledging \
             before the content write means a later turn discovers a broken chain \
             far from the request that actually failed (INV-34)",
        );
    assert_eq!(stored.response_id, id);
    assert!(
        canonical_items(&stored.input_items).contains("durable-marker"),
        "the persisted items must be the ones submitted"
    );

    // A rejected create must leave nothing behind: a partial write would be a
    // silent inconsistency that no error message accounts for.
    let ghost = ports.new_id();
    let ghost_rec = record(ports, &ghost, None, &tenant, true, ResponseStatus::Queued);
    let key = fresh_key();
    ports
        .ledger
        .create(ghost_rec.clone(), key.clone(), 1_000)
        .await
        .expect("first create");
    // Replaying the same key must not produce a second stored record.
    let replay = ports
        .ledger
        .create(ghost_rec, key, 1_000)
        .await
        .expect("replay");
    assert!(
        matches!(replay, CreateOutcome::Duplicate { .. }),
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
        .ledger
        .create(
            record(ports, &id, None, &tenant, true, ResponseStatus::Queued),
            fresh_key(),
            1_000,
        )
        .await
        .expect("create");
    assert!(matches!(created, CreateOutcome::Accepted { .. }));

    let mut handles = Vec::new();
    for _ in 0..RACERS {
        let ledger = ports.ledger.clone();
        let node = ports.node_tag.clone();
        handles.push(tokio::spawn(async move {
            ledger
                .claim(&node, AgentId(uuid::Uuid::new_v4()), 1_100, 30_000)
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
            w.attempt.0, 1,
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
        let ledger = ports.ledger.clone();
        let key = key.clone();
        // Each racer proposes a *different* id, as independent retries would.
        let candidate = ports.new_id();
        let rec = record(ports, &candidate, None, &tenant, true, ResponseStatus::Queued);
        handles.push(tokio::spawn(
            async move { ledger.create(rec, key, 2_000).await },
        ));
    }

    let mut accepted = Vec::new();
    let mut duplicates = Vec::new();
    for h in handles {
        match h.await.expect("create task panicked").expect("create call") {
            CreateOutcome::Accepted { response_id } => accepted.push(response_id),
            CreateOutcome::Duplicate { response_id } => duplicates.push(response_id),
            other => panic!("unexpected outcome under contention: {other:?}"),
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
        .read_after(&id, None, 128, 0)
        .await
        .expect("read back");
    assert_eq!(read.len(), RACERS);
    for (i, ev) in read.iter().enumerate() {
        assert_eq!(ev.sequence_number, i as u64);
    }
}

// -------------------------------------------------------------- entry points

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
            covers: &["FR-7", "INV-51", "SEC-2"],
            scope: CaseScope::Backend,
            asserts: "assert_cancel_conformance",
        },
        ContractCase {
            name: "orphan-reclaim",
            covers: &["FR-5", "FR-36", "FR-38", "CR-6", "INV-45"],
            scope: CaseScope::Backend,
            asserts: "assert_orphan_reclaim_conformance",
        },
        ContractCase {
            name: "context-chain",
            covers: &[
                "FR-15", "FR-16", "FR-17", "FR-18", "FR-19", "FR-21", "FR-22", "CR-9", "CR-10",
                "INV-41", "INV-42", "INV-43", "INV-49", "SEC-2", "SEC-3",
            ],
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
            name: "claim-locality",
            covers: &["FR-4"],
            scope: CaseScope::Backend,
            asserts: "assert_claim_locality",
        },
        ContractCase {
            name: "overload-integrity",
            covers: &["CR-8", "FR-33", "INV-29"],
            scope: CaseScope::Backend,
            asserts: "assert_overload_integrity",
        },
        ContractCase {
            name: "output-provenance",
            covers: &["FR-20", "CR-3", "CR-7", "INV-6"],
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
            name: "reconnect-backoff",
            covers: &["FR-35", "INV-33"],
            scope: CaseScope::Protocol,
            asserts: "assert_reconnect_backoff",
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
        "orphan-reclaim" => assert_orphan_reclaim_conformance(ports).await,
        "context-chain" => assert_context_conformance(ports).await,
        "integrity" => match &ports.integrity {
            Some(integrity) => assert_integrity_conformance(integrity.clone()).await,
            None => return CaseOutcome::Skipped("backend supplies no ContentIntegrity port"),
        },
        "claim-locality" => assert_claim_locality(ports).await,
        "overload-integrity" => assert_overload_integrity(ports).await,
        "output-provenance" => assert_output_provenance(ports).await,
        "durability-order" => assert_durability_order(ports).await,
        "concurrency" => assert_concurrency_conformance(ports).await,
        "chain-closure" => assert_output_items_are_valid_input(),
        "protocol-subset" => assert_protocol_subset_rejects(),
        "event-coalesce" => assert_event_coalescing(),
        "reconnect-backoff" => assert_reconnect_backoff(),
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
    let world = adapters_mem::MemWorld::new();
    PortSet {
        ledger: world.ledger.clone(),
        event_log: world.event_log.clone(),
        context: world.context.clone(),
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
        async fn append(&self, mut event: ResponseEvent) -> Result<u64, EventLogError> {
            use std::sync::atomic::Ordering;
            // Read...
            let seen = self.next.load(Ordering::SeqCst);
            // ...let another task in...
            tokio::task::yield_now().await;
            // ...then write. Two tasks can observe the same value.
            self.next.store(seen + 1, Ordering::SeqCst);
            event.sequence_number = seen;
            self.events.lock().expect("lock").push(event);
            Ok(seen)
        }

        async fn read_after(
            &self,
            _response_id: &ResponseId,
            starting_after: Option<u64>,
            limit: usize,
            _wait_ms: u64,
        ) -> Result<Vec<ResponseEvent>, EventLogError> {
            let g = self.events.lock().expect("lock");
            Ok(g.iter()
                .filter(|e| match starting_after {
                    Some(after) => e.sequence_number > after,
                    None => true,
                })
                .take(limit)
                .cloned()
                .collect())
        }

        async fn close(&self, _response_id: &ResponseId, _now_ms: u64, _retain_ms: u64)
            -> Result<(), EventLogError> {
            Ok(())
        }

        async fn sweep_expired(&self, _now_ms: u64) -> Result<u64, EventLogError> {
            Ok(0)
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
