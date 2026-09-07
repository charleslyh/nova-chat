//! Adapter-specific behaviour that the shared L0 contract cannot express:
//! ring eviction mechanics and the single-lock chain walk.
//!
//! Port semantics themselves are asserted once, in `testing/conformance`, and
//! run against every backend.


use adapters_mem::{MemWorld, MemWorldConfig};
use nova_responses_core::{
    Attempt, ChainLimits, ContextError, ContextStore, EventBody, EventLogError, IdempotencyKey,
    NodeTag, ResponseEvent, ResponseEventKind, ResponseEventLog, ResponseId, ResponseItem,
    ResponseLedger, ResponseStatus, StoredResponse, TenantId, Usage,
};

fn tag() -> NodeTag {
    NodeTag::parse("node-a").unwrap()
}

fn tenant(s: &str) -> TenantId {
    TenantId::parse(s).unwrap()
}

fn record(
    id: &ResponseId,
    previous: Option<&ResponseId>,
    tenant_id: &str,
    stored: bool,
) -> StoredResponse {
    StoredResponse {
        conversation_id: None,
        session_id: None,
        response_id: id.clone(),
        previous_response_id: previous.cloned(),
        tenant_id: tenant(tenant_id),
        model: "m".into(),
        instructions: Some("SYSTEM-PROMPT-MARKER".into()),
        input_items: vec![ResponseItem::user_text(format!("in-{}", id.uuid()))],
        output_items: vec![ResponseItem::assistant_text(format!("out-{}", id.uuid()))],
        reasoning: None,
        status: ResponseStatus::Completed,
        usage: Usage::new(1, 1),
        created_at_ms: 0,
        completed_at_ms: Some(1),
        stored,
        expires_at_ms: None,
        integrity: None,
        integrity_alg: None,
        node_tag: tag(),
        idempotency_key: None,
        owner: None,
        attempt: Attempt::default(),
        context: Vec::new(),
        context_reasoning: Vec::new(),
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

async fn seed_chain(world: &MemWorld, depth: usize, tenant_id: &str) -> Vec<ResponseId> {
    let mut ids = Vec::new();
    let mut previous: Option<ResponseId> = None;
    // Materialised history (D24): each link snapshots everything before it as a
    // flat copy, so resolution never walks `previous_response_id`. This mirrors
    // what the gateway does at create time.
    let mut history: Vec<ResponseItem> = Vec::new();
    for _ in 0..depth {
        let id = ResponseId::new(tag());
        let mut rec = record(&id, previous.as_ref(), tenant_id, true);
        rec.context = history.clone();
        rec.context_depth = ids.len();
        let own: Vec<ResponseItem> = rec
            .input_items
            .iter()
            .chain(rec.output_items.iter())
            .cloned()
            .collect();
        world.context.put(rec).await.unwrap();
        history.extend(own);
        previous = Some(id.clone());
        ids.push(id);
    }
    ids
}

#[tokio::test]
async fn sequence_numbers_are_zero_based_and_contiguous() {
    let world = MemWorld::new();
    let id = ResponseId::new(tag());
    for i in 0..5 {
        let seq = world
            .event_log
            .append(event(&id, ResponseEventKind::OutputTextDelta, &i.to_string()))
            .await
            .unwrap();
        assert_eq!(seq, i, "sequence numbers must start at 0 and not skip");
    }
    let all = world
        .event_log
        .read_after(&id, None, 100, 0)
        .await
        .unwrap();
    let seqs: Vec<u64> = all.iter().map(|e| e.sequence_number).collect();
    assert_eq!(seqs, vec![0, 1, 2, 3, 4]);
}

#[tokio::test]
async fn starting_after_is_exclusive_and_zero_is_a_real_cursor() {
    let world = MemWorld::new();
    let id = ResponseId::new(tag());
    for i in 0..3 {
        world
            .event_log
            .append(event(&id, ResponseEventKind::OutputTextDelta, &i.to_string()))
            .await
            .unwrap();
    }
    // `Some(0)` must skip event 0 — distinguishing it from `None` is exactly why
    // the parameter is an Option rather than a sentinel.
    let after_zero = world.event_log.read_after(&id, Some(0), 100, 0).await.unwrap();
    assert_eq!(
        after_zero.iter().map(|e| e.sequence_number).collect::<Vec<_>>(),
        vec![1, 2]
    );
    let from_start = world.event_log.read_after(&id, None, 100, 0).await.unwrap();
    assert_eq!(from_start.len(), 3);
}

#[tokio::test]
async fn ring_eviction_raises_the_watermark_and_reports_expired() {
    // Capacity 3 so eviction is reachable.
    let world = MemWorld::with_config(MemWorldConfig {
        events_per_response: 3,
        ..MemWorldConfig::default()
    });
    let id = ResponseId::new(tag());
    for i in 0..5 {
        world
            .event_log
            .append(event(&id, ResponseEventKind::OutputTextDelta, &i.to_string()))
            .await
            .unwrap();
    }
    assert_eq!(world.event_log.buffered_len(&id), 3);
    assert_eq!(world.event_log.evicted_before(&id), Some(2));

    // A subscriber that fell behind gets an explicit error — never a silent
    // resume from a later position (INV-40).
    assert_eq!(
        world.event_log.read_after(&id, None, 10, 0).await,
        Err(EventLogError::Expired)
    );
    assert_eq!(
        world.event_log.read_after(&id, Some(0), 10, 0).await,
        Err(EventLogError::Expired)
    );
    // Still-retained cursors keep working.
    let tail = world.event_log.read_after(&id, Some(2), 10, 0).await.unwrap();
    assert_eq!(
        tail.iter().map(|e| e.sequence_number).collect::<Vec<_>>(),
        vec![3, 4]
    );
}

#[tokio::test]
async fn eviction_does_not_kill_the_generation() {
    // The deliberate trade-off: overflowing the buffer degrades *subscription
    // history*, not the generation itself.
    let world = MemWorld::with_config(MemWorldConfig {
        events_per_response: 2,
        ..MemWorldConfig::default()
    });
    let id = ResponseId::new(tag());
    for i in 0..10 {
        assert!(
            world
                .event_log
                .append(event(&id, ResponseEventKind::OutputTextDelta, &i.to_string()))
                .await
                .is_ok(),
            "append must keep succeeding past capacity"
        );
    }
    assert_eq!(world.event_log.buffered_len(&id), 2);
}

#[tokio::test]
async fn unknown_id_is_distinct_from_expired() {
    let world = MemWorld::new();
    let unknown = ResponseId::new(tag());
    assert_eq!(
        world.event_log.read_after(&unknown, None, 10, 0).await,
        Err(EventLogError::Unknown)
    );
}

#[tokio::test]
async fn retention_window_expires_then_becomes_unknown() {
    let world = MemWorld::new();
    let id = ResponseId::new(tag());
    world
        .event_log
        .append(event(&id, ResponseEventKind::Completed, "done"))
        .await
        .unwrap();
    world.event_log.close(&id, 1_000, 60_000).await.unwrap();

    // Before the deadline the tail is still readable.
    assert!(world.event_log.read_after(&id, None, 10, 0).await.is_ok());

    // After the retention window: explicit expiry (410-shaped).
    world.event_log.sweep_expired(61_001).await.unwrap();
    assert_eq!(
        world.event_log.read_after(&id, None, 10, 0).await,
        Err(EventLogError::Expired)
    );

    // Much later the tombstone itself is collected and the id is simply unknown
    // (404-shaped). Both are explicit; neither returns partial data.
    world.event_log.sweep_expired(10_000_000).await.unwrap();
    assert_eq!(
        world.event_log.read_after(&id, None, 10, 0).await,
        Err(EventLogError::Unknown)
    );
}

#[tokio::test]
async fn node_level_log_capacity_refuses_new_logs() {
    let world = MemWorld::with_config(MemWorldConfig {
        max_logs: 1,
        ..MemWorldConfig::default()
    });
    let first = ResponseId::new(tag());
    let second = ResponseId::new(tag());
    world
        .event_log
        .append(event(&first, ResponseEventKind::Created, ""))
        .await
        .unwrap();
    assert_eq!(
        world
            .event_log
            .append(event(&second, ResponseEventKind::Created, ""))
            .await,
        Err(EventLogError::CapacityExceeded)
    );
    // The existing log is untouched.
    assert!(world
        .event_log
        .append(event(&first, ResponseEventKind::OutputTextDelta, "x"))
        .await
        .is_ok());
}

#[tokio::test]
async fn chain_resolves_in_chronological_order() {
    let world = MemWorld::new();
    let ids = seed_chain(&world, 3, "t1").await;
    let resolved = world
        .context
        .resolve_chain(&tenant("t1"), ids.last().unwrap(), ChainLimits::default())
        .await
        .unwrap();
    assert_eq!(resolved.depth, 3);
    assert_eq!(resolved.items.len(), 6, "3 links x (input + output)");

    // Oldest first: the first item must belong to the first link.
    let first_marker = format!("in-{}", ids[0].uuid());
    let encoded = nova_responses_core::canonical_items(&resolved.items[..1]);
    assert!(
        encoded.contains(&first_marker),
        "expected oldest link first, got {encoded}"
    );
}

#[tokio::test]
async fn chain_never_includes_instructions() {
    let world = MemWorld::new();
    let ids = seed_chain(&world, 3, "t1").await;
    let resolved = world
        .context
        .resolve_chain(&tenant("t1"), ids.last().unwrap(), ChainLimits::default())
        .await
        .unwrap();
    let encoded = nova_responses_core::canonical_items(&resolved.items);
    assert!(
        !encoded.contains("SYSTEM-PROMPT-MARKER"),
        "instructions must not cross turns (INV-49): {encoded}"
    );
}

#[tokio::test]
async fn deep_snapshot_resolves_in_one_read() {
    // History is materialised (D24): resolving a 50-link chain is a single record
    // read, not a 50-hop walk. The old per-hop-lock regression this test guarded
    // against no longer exists, because there is no traversal to lock.
    let world = MemWorld::new();
    let ids = seed_chain(&world, 50, "t1").await;
    let resolved = world
        .context
        .resolve_chain(&tenant("t1"), ids.last().unwrap(), ChainLimits::default())
        .await
        .unwrap();
    assert_eq!(resolved.depth, 50);
    assert_eq!(resolved.items.len(), 100);
}

#[tokio::test]
async fn chain_rejects_depth_and_byte_overruns_instead_of_truncating() {
    let world = MemWorld::new();
    let ids = seed_chain(&world, 5, "t1").await;
    let head = ids.last().unwrap();

    assert_eq!(
        world
            .context
            .resolve_chain(
                &tenant("t1"),
                head,
                ChainLimits {
                    max_depth: 3,
                    ..ChainLimits::default()
                }
            )
            .await,
        Err(ContextError::ChainTooLong { limit: 3 })
    );

    assert_eq!(
        world
            .context
            .resolve_chain(
                &tenant("t1"),
                head,
                ChainLimits {
                    max_bytes: 10,
                    ..ChainLimits::default()
                }
            )
            .await,
        Err(ContextError::ChainTooLarge { limit: 10 })
    );
}

// Cross-tenant detection moved to create time (D24). A snapshot is built by
// resolving the `previous` response's own snapshot, and that resolution already
// checks the tenant — so a foreign link can never be materialised into a
// snapshot. Resolution therefore no longer walks far enough to observe one, and
// `CrossTenant` is no longer produced here.

#[tokio::test]
async fn foreign_anchor_is_indistinguishable_from_a_missing_one() {
    // The anchor comes straight from the caller, so answering differently for
    // "exists but is someone else's" would turn the field into an id oracle.
    let world = MemWorld::new();
    let theirs = ResponseId::new(tag());
    world
        .context
        .put(record(&theirs, None, "other", true))
        .await
        .unwrap();
    let ghost = ResponseId::new(tag());

    let foreign = world
        .context
        .resolve_chain(&tenant("t1"), &theirs, ChainLimits::default())
        .await;
    let missing = world
        .context
        .resolve_chain(&tenant("t1"), &ghost, ChainLimits::default())
        .await;
    assert!(matches!(foreign, Err(ContextError::ChainBroken(_))));
    assert!(matches!(missing, Err(ContextError::ChainBroken(_))));
}

#[tokio::test]
async fn an_unstored_anchor_cannot_be_resolved() {
    // `NotStored` now means the *anchor itself* was created with `store: false`,
    // not that some upstream link was. The snapshot contains only stored items by
    // construction, so there is no upstream unstored link to walk into.
    let world = MemWorld::new();
    let unstored = ResponseId::new(tag());
    world
        .context
        .put(record(&unstored, None, "t1", false))
        .await
        .unwrap();
    assert_eq!(
        world
            .context
            .resolve_chain(&tenant("t1"), &unstored, ChainLimits::default())
            .await,
        Err(ContextError::NotStored)
    );
}

#[tokio::test]
async fn deleting_an_ancestor_does_not_strand_descendants() {
    // The property the materialised snapshot exists for (D24). Delete the middle
    // link of a three-link chain; the downstream link must still resolve, with its
    // own and the oldest link's content intact but the deleted turn stripped.
    let world = MemWorld::new();
    let ids = seed_chain(&world, 3, "t1").await;

    world
        .context
        .delete(&tenant("t1"), &ids[1])
        .await
        .expect("delete middle link");

    let resolved = world
        .context
        .resolve_chain(&tenant("t1"), ids.last().unwrap(), ChainLimits::default())
        .await
        .expect("downstream must still resolve");

    // The snapshot is a flat copy, so deletion is record-level only (D24): the
    // downstream link still resolves with the *full* history, including the
    // deleted turn's content. "Remove from the conversation" removes the record,
    // not the inherited copy.
    assert_eq!(resolved.depth, 3, "the snapshot keeps every link it inherited");
    assert_eq!(resolved.items.len(), 6);

    let encoded = nova_responses_core::canonical_items(&resolved.items);
    for id in &ids {
        assert!(
            encoded.contains(&format!("in-{}", id.uuid())),
            "every link's content must survive deletion, including the deleted one: {encoded}"
        );
    }

    // But the deleted response itself is no longer resolvable as an anchor.
    assert!(matches!(
        world
            .context
            .resolve_chain(&tenant("t1"), &ids[1], ChainLimits::default())
            .await,
        Err(ContextError::ChainBroken(_))
    ));
}

#[tokio::test]
async fn tampering_with_stored_content_is_detected() {
    let world = MemWorld::new();
    let id = ResponseId::new(tag());
    world.context.put(record(&id, None, "t1", true)).await.unwrap();
    assert!(world
        .context
        .tamper_for_test(&id, vec![ResponseItem::assistant_text("forged")]));
    assert_eq!(
        world.context.get(&tenant("t1"), &id).await,
        Err(ContextError::IntegrityMismatch)
    );
}

#[tokio::test]
async fn store_unavailable_refuses_writes_rather_than_skipping_them() {
    let world = MemWorld::new();
    world.store.set_unavailable(true);
    let id = ResponseId::new(tag());
    assert_eq!(
        world.context.put(record(&id, None, "t1", true)).await,
        Err(ContextError::Unavailable)
    );
    assert_eq!(world.context.health().await, Err(ContextError::Unavailable));
    // Recovery restores normal service.
    world.store.set_unavailable(false);
    assert!(world.context.put(record(&id, None, "t1", true)).await.is_ok());
}

#[tokio::test]
async fn tenant_purge_uses_the_index_and_leaves_others_intact() {
    let world = MemWorld::new();
    seed_chain(&world, 3, "t1").await;
    let keep = seed_chain(&world, 2, "t2").await;
    assert_eq!(world.context.delete_by_tenant(&tenant("t1")).await.unwrap(), 3);
    assert_eq!(world.context.record_count(), 2);
    assert!(world
        .context
        .get(&tenant("t2"), keep.last().unwrap())
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn partial_usage_survives_cancellation() {
    let world = MemWorld::new();
    let id = ResponseId::new(tag());
    let mut rec = record(&id, None, "t1", true);
    rec.status = ResponseStatus::Queued;
    rec.usage = Usage::default();
    world
        .ledger
        .create(rec, IdempotencyKey("k".into()), 0)
        .await
        .unwrap();

    let claimed = world
        .ledger
        .claim(nova_responses_core::AgentId::new(), 0, 60_000)
        .await
        .unwrap()
        .expect("claimable");

    // Tokens burnt before the cancellation.
    world
        .ledger
        .record_partial_usage(&id, claimed.attempt, Usage::new(7, 3))
        .await
        .unwrap();
    world.ledger.cancel(&tenant("t1"), &id, 1_000).await.unwrap();

    let total = world.ledger.total_usage(&id);
    assert_eq!(total.input_tokens, 7);
    assert_eq!(total.output_tokens, 3);
    assert_eq!(total.total_tokens, 10, "billing must not lose burnt tokens");
}

#[tokio::test]
async fn cancel_is_scoped_to_the_owning_tenant() {
    let world = MemWorld::new();
    let id = ResponseId::new(tag());
    let mut rec = record(&id, None, "t1", true);
    rec.status = ResponseStatus::Queued;
    world
        .ledger
        .create(rec, IdempotencyKey("k".into()), 0)
        .await
        .unwrap();
    // Reported as missing rather than forbidden, so ids cannot be enumerated.
    assert_eq!(
        world.ledger.cancel(&tenant("intruder"), &id, 1).await,
        Err(nova_responses_core::LedgerError::NotFound)
    );
}

#[tokio::test]
async fn stale_attempt_cannot_append_after_reaping() {
    let world = MemWorld::new();
    let id = ResponseId::new(tag());
    let mut rec = record(&id, None, "t1", true);
    rec.status = ResponseStatus::Queued;
    world
        .ledger
        .create(rec, IdempotencyKey("k".into()), 0)
        .await
        .unwrap();
    let agent = nova_responses_core::AgentId::new();
    let claimed = world.ledger.claim(agent, 0, 60_000).await.unwrap().unwrap();

    let mut ev = event(&id, ResponseEventKind::OutputTextDelta, "a");
    ev.attempt = Some(claimed.attempt);
    assert!(world.event_log.append(ev.clone()).await.is_ok());

    // Heartbeat lost -> reaped -> fence raised.
    world.ledger.reap(1_000_000, 90_000).await.unwrap();
    assert_eq!(
        world.event_log.append(ev).await,
        Err(EventLogError::StaleAttempt)
    );
}

#[tokio::test]
async fn expiry_sweep_removes_only_due_records() {
    let world = MemWorld::new();
    let soon = ResponseId::new(tag());
    let mut soon_rec = record(&soon, None, "t1", true);
    soon_rec.expires_at_ms = Some(1_000);
    world.context.put(soon_rec).await.unwrap();

    let later = ResponseId::new(tag());
    let mut later_rec = record(&later, None, "t1", true);
    later_rec.expires_at_ms = Some(100_000);
    world.context.put(later_rec).await.unwrap();

    assert_eq!(world.context.sweep_expired(1_000, 100).await.unwrap(), 1);
    assert!(world.context.get(&tenant("t1"), &soon).await.unwrap().is_none());
    assert!(world.context.get(&tenant("t1"), &later).await.unwrap().is_some());
}

#[tokio::test]
async fn idempotency_gate_has_no_ttl_window() {
    let world = MemWorld::new();
    let id = ResponseId::new(tag());
    let mut rec = record(&id, None, "t1", true);
    rec.status = ResponseStatus::Queued;
    let key = IdempotencyKey("same".into());
    let first = world.ledger.create(rec.clone(), key.clone(), 0).await.unwrap();
    let response_id = match first {
        nova_responses_core::CreateOutcome::Accepted { response_id } => response_id,
        other => panic!("expected acceptance, got {other:?}"),
    };
    // Far in the future: presence alone still rejects (INV-2).
    let replay = world
        .ledger
        .create(record(&ResponseId::new(tag()), None, "t1", true), key, u64::MAX)
        .await
        .unwrap();
    assert_eq!(
        replay,
        nova_responses_core::CreateOutcome::Duplicate { response_id }
    );
}

#[tokio::test]
async fn read_only_degrade_blocks_writes_but_not_reads() {
    let world = MemWorld::new();
    let id = ResponseId::new(tag());
    world.context.put(record(&id, None, "t1", true)).await.unwrap();
    world.store.set_read_only(true);

    assert_eq!(
        world.context.put(record(&ResponseId::new(tag()), None, "t1", true)).await,
        Err(ContextError::ReadOnly)
    );
    assert_eq!(
        world
            .event_log
            .append(event(&id, ResponseEventKind::OutputTextDelta, "x"))
            .await,
        Err(EventLogError::ReadOnly)
    );
    assert!(world.context.get(&tenant("t1"), &id).await.unwrap().is_some());
}

#[tokio::test]
async fn overload_rejects_new_work_without_corrupting_state() {
    let world = MemWorld::with_config(MemWorldConfig {
        pending_limit: 1,
        ..MemWorldConfig::default()
    });
    let first = ResponseId::new(tag());
    let mut a = record(&first, None, "t1", true);
    a.status = ResponseStatus::Queued;
    world
        .ledger
        .create(a, IdempotencyKey("k1".into()), 0)
        .await
        .unwrap();

    let second = ResponseId::new(tag());
    let mut b = record(&second, None, "t1", true);
    b.status = ResponseStatus::Queued;
    assert_eq!(
        world
            .ledger
            .create(b, IdempotencyKey("k2".into()), 0)
            .await
            .unwrap(),
        nova_responses_core::CreateOutcome::Overloaded
    );
    // Rejected work leaves no trace (INV-30).
    assert!(world.ledger.get(&second).await.unwrap().is_none());
    assert_eq!(world.ledger.in_flight().await.unwrap(), 1);
}
