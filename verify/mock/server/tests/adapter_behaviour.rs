//! Adapter-specific behaviour that the shared L0 contract cannot express:
//! ring eviction mechanics and the conversation snapshot accumulation.
//!
//! Port semantics themselves are asserted once, in `verify/conformance`, and
//! run against every backend.

use std::time::Duration;

use mock_server::{MemWorld, MemWorldConfig};
use nova_responses::ports::{
    ConversationRepo, ConversationSnapshots, EventLogError, ResponseEventLog, ResponseLedger,
    StoreError,
};
use nova_responses::{
    AppendEvent, Attempt, ContextAnchor, Conversation, ConversationId, EventBody, IdempotencyKey,
    ModelParams, NodeTag, ResponseEventKind, ResponseId, ResponseItem, ResponseRecord,
    ResponseStatus, TenantId, TurnCommit, TurnSpec, Usage,
};

fn tag() -> NodeTag {
    NodeTag::parse("node-a").unwrap()
}

fn tenant(s: &str) -> TenantId {
    TenantId::parse(s).unwrap()
}

fn record(id: &ResponseId, tenant_id: &str, stored: bool) -> ResponseRecord {
    ResponseRecord::queued(
        id.clone(),
        tenant(tenant_id),
        TurnSpec {
            params: ModelParams {
                instructions: Some("SYSTEM-PROMPT-MARKER".into()),
                ..ModelParams::new("m")
            },
            input_items: vec![ResponseItem::user_text(format!("in-{}", id.uuid()))],
            store: stored,
            ext: None,
            anchor: ContextAnchor::Root,
        },
        IdempotencyKey::parse(&id.to_string()).expect("a response id is a valid key"),
        0,
        0,
    )
}

fn event(id: &ResponseId, kind: ResponseEventKind, payload: &str) -> AppendEvent {
    fenced_event(id, kind, None, payload)
}

/// The same, carrying an explicit fence. Built through the domain constructor, so a kind
/// can never be paired with a body that does not belong to it.
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

/// Seed a conversation with `turns` completed turns (each contributing an input
/// and an output item), returning its id and the turn response ids in order.
async fn seed_conversation(
    world: &MemWorld,
    turns: usize,
    tenant_id: &str,
) -> (ConversationId, Vec<ResponseId>) {
    let conversation = world
        .conversation
        .create(Conversation::new(
            ConversationId::new(),
            tenant(tenant_id),
            Default::default(),
            1_000,
        ))
        .await
        .expect("create conversation");
    let mut ids = Vec::new();
    for i in 0..turns {
        let id = ResponseId::new(tag());
        world
            .conversation
            .append_turn(
                &tenant(tenant_id),
                &conversation.id,
                &id,
                TurnCommit {
                    input_items: vec![ResponseItem::user_text(format!("in-{i}"))],
                    output_items: vec![ResponseItem::assistant_text(format!("out-{i}"))],
                    reasoning: None,
                    usage: Usage::new(1, 1),
                    status: ResponseStatus::Completed,
                },
                0,
            )
            .await
            .expect("append turn");
        ids.push(id);
    }
    (conversation.id, ids)
}

// ---------------------------------------------------------------------------
// Event log mechanics (unchanged by D30).
// ---------------------------------------------------------------------------

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
        .read_after(&id, None, 100, Duration::from_millis(0))
        .await
        .unwrap();
    let seqs: Vec<u64> = all.iter().map(|e| e.sequence_number()).collect();
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
    let after_zero = world.event_log.read_after(&id, Some(0), 100, Duration::from_millis(0)).await.unwrap();
    assert_eq!(
        after_zero.iter().map(|e| e.sequence_number()).collect::<Vec<_>>(),
        vec![1, 2]
    );
    let from_start = world.event_log.read_after(&id, None, 100, Duration::from_millis(0)).await.unwrap();
    assert_eq!(from_start.len(), 3);
}

#[tokio::test]
async fn ring_eviction_raises_the_watermark_and_reports_expired() {
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

    assert_eq!(
        world.event_log.read_after(&id, None, 10, Duration::from_millis(0)).await,
        Err(EventLogError::Expired)
    );
    assert_eq!(
        world.event_log.read_after(&id, Some(0), 10, Duration::from_millis(0)).await,
        Err(EventLogError::Expired)
    );
    let tail = world.event_log.read_after(&id, Some(2), 10, Duration::from_millis(0)).await.unwrap();
    assert_eq!(
        tail.iter().map(|e| e.sequence_number()).collect::<Vec<_>>(),
        vec![3, 4]
    );
}

#[tokio::test]
async fn eviction_does_not_kill_the_generation() {
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
        world.event_log.read_after(&unknown, None, 10, Duration::from_millis(0)).await,
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
    world.event_log.close(&id, 1_000, Duration::from_millis(60_000)).await.unwrap();

    assert!(world.event_log.read_after(&id, None, 10, Duration::from_millis(0)).await.is_ok());

    world.event_log.sweep_expired(61_001).await.unwrap();
    assert_eq!(
        world.event_log.read_after(&id, None, 10, Duration::from_millis(0)).await,
        Err(EventLogError::Expired)
    );

    world.event_log.sweep_expired(10_000_000).await.unwrap();
    assert_eq!(
        world.event_log.read_after(&id, None, 10, Duration::from_millis(0)).await,
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
    assert!(world
        .event_log
        .append(event(&first, ResponseEventKind::OutputTextDelta, "x"))
        .await
        .is_ok());
}

// ---------------------------------------------------------------------------
// Conversation snapshot (D30): the long-term record of a dialogue.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn snapshot_accumulates_turns_in_chronological_order() {
    let world = MemWorld::new();
    let (conv, ids) = seed_conversation(&world, 3, "t1").await;
    let snap = world
        .conversation
        .read_snapshot(&tenant("t1"), &conv)
        .await
        .unwrap();
    assert_eq!(snap.turns, 3);
    assert_eq!(snap.item_count(), 6, "3 turns x (input + output)");
    assert!(ids.len() == 3);
    // Oldest first.
    assert!(nova_responses::canonical_items(&snap.clone().into_items()[..1]).contains("in-0"));
}

#[tokio::test]
async fn append_turn_is_idempotent_per_response() {
    // The runtime's terminal funnel and the service layer's cancel/reap funnel can both
    // attempt the same turn; the second append must return the assigned index without
    // duplicating entries.
    let world = MemWorld::new();
    let conversation = world
        .conversation
        .create(Conversation::new(
            ConversationId::new(),
            tenant("t1"),
            Default::default(),
            1_000,
        ))
        .await
        .expect("create");
    let id = ResponseId::new(tag());
    let commit = TurnCommit {
        input_items: vec![ResponseItem::user_text("q")],
        output_items: vec![ResponseItem::assistant_text("a")],
        reasoning: None,
        usage: Usage::new(1, 1),
        status: ResponseStatus::Completed,
    };
    let first = world
        .conversation
        .append_turn(&tenant("t1"), &conversation.id, &id, commit.clone(), 0)
        .await
        .expect("first append");
    let second = world
        .conversation
        .append_turn(&tenant("t1"), &conversation.id, &id, commit, 0)
        .await
        .expect("second append");
    assert_eq!(first, second, "a repeat append returns the assigned index");
    let snap = world
        .conversation
        .read_snapshot(&tenant("t1"), &conversation.id)
        .await
        .unwrap();
    assert_eq!(snap.turns, 1, "the repeat must not add a turn");
    assert_eq!(snap.item_count(), 2, "the repeat must not duplicate items");
}

#[tokio::test]
async fn an_input_only_turn_is_archived() {
    // A failed/cancelled/reaped turn commits no output, but its input must still land
    // in the snapshot (D30 incomplete-turn archival) so the chain keeps the question.
    let world = MemWorld::new();
    let conversation = world
        .conversation
        .create(Conversation::new(
            ConversationId::new(),
            tenant("t1"),
            Default::default(),
            1_000,
        ))
        .await
        .expect("create");
    let id = ResponseId::new(tag());
    world
        .conversation
        .append_turn(
            &tenant("t1"),
            &conversation.id,
            &id,
            TurnCommit {
                input_items: vec![ResponseItem::user_text("unanswered")],
                output_items: vec![],
                reasoning: None,
                usage: Usage::default(),
                status: ResponseStatus::Failed,
            },
            0,
        )
        .await
        .expect("append");
    let snap = world
        .conversation
        .read_snapshot(&tenant("t1"), &conversation.id)
        .await
        .unwrap();
    assert_eq!(snap.turns, 1);
    assert_eq!(snap.item_count(), 1);
    assert!(nova_responses::canonical_items(&snap.clone().into_items()).contains("unanswered"));
}

#[tokio::test]
async fn snapshot_never_contains_instructions() {
    // INV-49: instructions live on the response record, never in the snapshot.
    let world = MemWorld::new();
    let (conv, _) = seed_conversation(&world, 2, "t1").await;
    let snap = world
        .conversation
        .read_snapshot(&tenant("t1"), &conv)
        .await
        .unwrap();
    let encoded = nova_responses::canonical_items(&snap.clone().into_items());
    assert!(
        !encoded.contains("SYSTEM-PROMPT-MARKER"),
        "instructions must not enter the snapshot: {encoded}"
    );
}

#[tokio::test]
async fn snapshot_survives_response_deletion() {
    // The whole point of the durable snapshot (D30): deleting a response record
    // does not erase the content the conversation already inherited.
    let world = MemWorld::new();
    let (conv, ids) = seed_conversation(&world, 2, "t1").await;

    world.ledger.delete(&ids[0]).await.expect("delete first response");
    let snap = world
        .conversation
        .read_snapshot(&tenant("t1"), &conv)
        .await
        .unwrap();
    assert_eq!(snap.turns, 2, "the snapshot keeps every turn it accumulated");
    assert_eq!(snap.item_count(), 4);
}

#[tokio::test]
async fn a_missing_conversation_is_not_found() {
    let world = MemWorld::new();
    let ghost = ConversationId::new();
    assert_eq!(
        world
            .conversation
            .read_snapshot(&tenant("t1"), &ghost)
            .await,
        Err(nova_responses::ports::ConversationError::NotFound)
    );
}

// ---------------------------------------------------------------------------
// Ledger mechanics (unchanged by D30, plus record-level delete).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn partial_usage_survives_cancellation() {
    let world = MemWorld::new();
    let id = ResponseId::new(tag());
    world
        .ledger
        .create(record(&id, "t1", true), IdempotencyKey::parse("k").unwrap(), 0)
        .await
        .unwrap();

    let claimed = world
        .ledger
        .claim(nova_responses::AgentId::new(), 0, Duration::from_millis(60_000))
        .await
        .unwrap()
        .expect("claimable");

    world
        .ledger
        .record_partial_usage(&id, claimed.record.attempt, Usage::new(7, 3))
        .await
        .unwrap();
    world.ledger.cancel(&tenant("t1"), &id, 1_000).await.unwrap();

    let total = world.ledger.total_usage(&id);
    assert_eq!(total.input_tokens, 7);
    assert_eq!(total.output_tokens, 3);
    assert_eq!(total.total_tokens(), 10, "billing must not lose burnt tokens");
}

#[tokio::test]
async fn cancel_is_scoped_to_the_owning_tenant() {
    let world = MemWorld::new();
    let id = ResponseId::new(tag());
    world
        .ledger
        .create(record(&id, "t1", true), IdempotencyKey::parse("k").unwrap(), 0)
        .await
        .unwrap();
    assert_eq!(
        world.ledger.cancel(&tenant("intruder"), &id, 1).await,
        Err(nova_responses::ports::LedgerError::NotFound)
    );
}

#[tokio::test]
async fn stale_attempt_cannot_append_after_reaping() {
    let world = MemWorld::new();
    let id = ResponseId::new(tag());
    world
        .ledger
        .create(record(&id, "t1", true), IdempotencyKey::parse("k").unwrap(), 0)
        .await
        .unwrap();
    let agent = nova_responses::AgentId::new();
    let claimed = world.ledger.claim(agent, 0, Duration::from_millis(60_000)).await.unwrap().unwrap();

    let ev = fenced_event(
        &id,
        ResponseEventKind::OutputTextDelta,
        Some(claimed.record.attempt),
        "a",
    );
    assert!(world.event_log.append(ev.clone()).await.is_ok());

    world.ledger.reap(1_000_000, Duration::from_millis(90_000)).await.unwrap();
    assert_eq!(
        world.event_log.append(ev).await,
        Err(EventLogError::StaleAttempt)
    );
}

#[tokio::test]
async fn reap_carries_the_turns_input_and_store_flag() {
    // The sweeper archives a reaped turn from what `reap` returns, so the input and
    // store flag must travel with the claim (D30 incomplete-turn archival).
    let world = MemWorld::new();
    let conversation = world
        .conversation
        .create(Conversation::new(
            ConversationId::new(),
            tenant("t1"),
            Default::default(),
            1_000,
        ))
        .await
        .expect("create conversation");

    let id = ResponseId::new(tag());
    let mut rec = record(&id, "t1", true);
    rec.spec.anchor = ContextAnchor::Conversation(conversation.id.clone());
    world
        .ledger
        .create(rec, IdempotencyKey::parse("k").unwrap(), 0)
        .await
        .unwrap();

    let agent = nova_responses::AgentId::new();
    world
        .ledger
        .claim(agent, 0, Duration::from_millis(60_000))
        .await
        .unwrap()
        .unwrap();

    // No heartbeat was ever recorded, so the claim is immediately lost and reaped.
    let aborted = world
        .ledger
        .reap(1_000, Duration::from_millis(0))
        .await
        .unwrap();
    assert_eq!(aborted.len(), 1);
    assert_eq!(aborted[0].response_id, id);
    assert_eq!(aborted[0].conversation_id, Some(conversation.id.clone()));
    assert!(aborted[0].store, "the store flag must travel with the claim");
    assert_eq!(
        aborted[0].input_items.len(),
        1,
        "the turn's input must travel with the claim"
    );
}

#[tokio::test]
async fn idempotency_gate_has_no_ttl_window() {
    let world = MemWorld::new();
    let id = ResponseId::new(tag());
    let key = IdempotencyKey::parse("same").unwrap();
    let first = world
        .ledger
        .create(record(&id, "t1", true), key.clone(), 0)
        .await
        .unwrap();
    let response_id = match first {
        nova_responses::ports::CreateOutcome::Accepted(record) => record.response_id.clone(),
        other => panic!("expected acceptance, got {other:?}"),
    };
    let replay = world
        .ledger
        .create(record(&ResponseId::new(tag()), "t1", true), key, u64::MAX)
        .await
        .unwrap();
    assert_eq!(
        replay.record().map(|r| r.response_id.clone()),
        Some(response_id),
        "an idempotent replay returns the original record"
    );
}

#[tokio::test]
async fn read_only_degrade_blocks_writes_but_not_reads() {
    let world = MemWorld::new();
    let id = ResponseId::new(tag());
    world
        .ledger
        .create(record(&id, "t1", true), IdempotencyKey::parse("k").unwrap(), 0)
        .await
        .unwrap();
    world.store.set_read_only(true);

    assert_eq!(
        world
            .ledger
            .create(record(&ResponseId::new(tag()), "t1", true), IdempotencyKey::parse("k2").unwrap(), 0)
            .await,
        Ok(nova_responses::ports::CreateOutcome::ReadOnly)
    );
    assert_eq!(
        world
            .event_log
            .append(event(&id, ResponseEventKind::OutputTextDelta, "x"))
            .await,
        Err(EventLogError::Store(StoreError::ReadOnly))
    );
    assert!(world.ledger.get(&id).await.unwrap().is_some());
}

#[tokio::test]
async fn overload_rejects_new_work_without_corrupting_state() {
    let world = MemWorld::with_config(MemWorldConfig {
        pending_limit: 1,
        ..MemWorldConfig::default()
    });
    let first = ResponseId::new(tag());
    world
        .ledger
        .create(record(&first, "t1", true), IdempotencyKey::parse("k1").unwrap(), 0)
        .await
        .unwrap();

    let second = ResponseId::new(tag());
    assert_eq!(
        world
            .ledger
            .create(record(&second, "t1", true), IdempotencyKey::parse("k2").unwrap(), 0)
            .await
            .unwrap(),
        nova_responses::ports::CreateOutcome::Overloaded
    );
    assert!(world.ledger.get(&second).await.unwrap().is_none());
    assert_eq!(world.ledger.in_flight().await.unwrap(), 1);
}

#[tokio::test]
async fn tenant_purge_removes_records_and_snapshots() {
    let world = MemWorld::new();
    let (conv, ids) = seed_conversation(&world, 2, "t1").await;
    // Register the responses in the ledger too, so purge has records to remove.
    for id in &ids {
        let mut rec = record(id, "t1", true);
        rec.status = ResponseStatus::Completed;
        world
            .ledger
            .create(rec, IdempotencyKey::parse(&format!("k-{id}")).expect("generated key is valid"), 0)
            .await
            .unwrap();
    }
    let keep = ResponseId::new(tag());
    world
        .ledger
        .create(record(&keep, "t2", true), IdempotencyKey::parse("keep").unwrap(), 0)
        .await
        .unwrap();

    assert_eq!(world.ledger.delete_by_tenant(&tenant("t1")).await.unwrap(), 2);
    assert_eq!(world.conversation.delete_by_tenant(&tenant("t1")).await.unwrap(), 1);
    // The other tenant's records survive.
    assert!(world.ledger.get(&keep).await.unwrap().is_some());
    assert!(world.conversation.get(&tenant("t2"), &conv).await.unwrap().is_none());
}
