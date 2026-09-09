//! Adapter-specific behaviour that the shared L0 contract cannot express:
//! ring eviction mechanics and the conversation snapshot accumulation.
//!
//! Port semantics themselves are asserted once, in `verify/conformance`, and
//! run against every backend.

use mock_server::{MemWorld, MemWorldConfig};
use nova_responses::{
    AppendEvent, Attempt, Conversation, ConversationId, ConversationStore, EventBody,
    EventLogError, IdempotencyKey, NodeTag, ResponseEventKind, ResponseEventLog, ResponseId,
    ResponseItem, ResponseLedger, ResponseRecord, ResponseStatus, StoreError, TenantId, TurnCommit,
    Usage,
};

fn tag() -> NodeTag {
    NodeTag::parse("node-a").unwrap()
}

fn tenant(s: &str) -> TenantId {
    TenantId::parse(s).unwrap()
}

fn record(id: &ResponseId, tenant_id: &str, stored: bool) -> ResponseRecord {
    ResponseRecord {
        conversation_id: None,
        response_id: id.clone(),
        previous_response_id: None,
        tenant_id: tenant(tenant_id),
        model: "m".into(),
        instructions: Some("SYSTEM-PROMPT-MARKER".into()),
        tools: Vec::new(),
        tool_choice: None,
        input_items: vec![ResponseItem::user_text(format!("in-{}", id.uuid()))],
        reasoning: None,
        status: ResponseStatus::Queued,
        usage: Usage::default(),
        created_at_ms: 0,
        completed_at_ms: None,
        stored,
        expires_at_ms: None,
        integrity: None,
        integrity_alg: None,
        idempotency_key: None,
        owner: None,
        attempt: Attempt::default(),
    }
}

fn event(id: &ResponseId, kind: ResponseEventKind, payload: &str) -> AppendEvent {
    AppendEvent {
        response_id: id.clone(),
        kind,
        attempt: None,
        body: EventBody::Delta {
            item_id: String::new(),
            output_index: 0,
            content_index: None,
            delta: payload.to_string(),
        },
    }
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
        world.event_log.read_after(&id, None, 10, 0).await,
        Err(EventLogError::Expired)
    );
    assert_eq!(
        world.event_log.read_after(&id, Some(0), 10, 0).await,
        Err(EventLogError::Expired)
    );
    let tail = world.event_log.read_after(&id, Some(2), 10, 0).await.unwrap();
    assert_eq!(
        tail.iter().map(|e| e.sequence_number).collect::<Vec<_>>(),
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

    assert!(world.event_log.read_after(&id, None, 10, 0).await.is_ok());

    world.event_log.sweep_expired(61_001).await.unwrap();
    assert_eq!(
        world.event_log.read_after(&id, None, 10, 0).await,
        Err(EventLogError::Expired)
    );

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
    assert_eq!(snap.depth, 3);
    assert_eq!(snap.items.len(), 6, "3 turns x (input + output)");
    assert!(ids.len() == 3);
    // Oldest first.
    assert!(nova_responses::canonical_items(&snap.items[..1]).contains("in-0"));
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
    let encoded = nova_responses::canonical_items(&snap.items);
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
    assert_eq!(snap.depth, 2, "the snapshot keeps every turn it accumulated");
    assert_eq!(snap.items.len(), 4);
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
        Err(nova_responses::ConversationError::NotFound)
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
        .create(record(&id, "t1", true), IdempotencyKey("k".into()), 0)
        .await
        .unwrap();

    let claimed = world
        .ledger
        .claim(nova_responses::AgentId::new(), 0, 60_000)
        .await
        .unwrap()
        .expect("claimable");

    world
        .ledger
        .record_partial_usage(&id, claimed.attempt, Usage::new(7, 3))
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
        .create(record(&id, "t1", true), IdempotencyKey("k".into()), 0)
        .await
        .unwrap();
    assert_eq!(
        world.ledger.cancel(&tenant("intruder"), &id, 1).await,
        Err(nova_responses::LedgerError::NotFound)
    );
}

#[tokio::test]
async fn stale_attempt_cannot_append_after_reaping() {
    let world = MemWorld::new();
    let id = ResponseId::new(tag());
    world
        .ledger
        .create(record(&id, "t1", true), IdempotencyKey("k".into()), 0)
        .await
        .unwrap();
    let agent = nova_responses::AgentId::new();
    let claimed = world.ledger.claim(agent, 0, 60_000).await.unwrap().unwrap();

    let mut ev = event(&id, ResponseEventKind::OutputTextDelta, "a");
    ev.attempt = Some(claimed.attempt);
    assert!(world.event_log.append(ev.clone()).await.is_ok());

    world.ledger.reap(1_000_000, 90_000).await.unwrap();
    assert_eq!(
        world.event_log.append(ev).await,
        Err(EventLogError::StaleAttempt)
    );
}

#[tokio::test]
async fn idempotency_gate_has_no_ttl_window() {
    let world = MemWorld::new();
    let id = ResponseId::new(tag());
    let key = IdempotencyKey("same".into());
    let first = world
        .ledger
        .create(record(&id, "t1", true), key.clone(), 0)
        .await
        .unwrap();
    let response_id = match first {
        nova_responses::CreateOutcome::Accepted { response_id } => response_id,
        other => panic!("expected acceptance, got {other:?}"),
    };
    let replay = world
        .ledger
        .create(record(&ResponseId::new(tag()), "t1", true), key, u64::MAX)
        .await
        .unwrap();
    assert_eq!(replay, nova_responses::CreateOutcome::Duplicate { response_id });
}

#[tokio::test]
async fn read_only_degrade_blocks_writes_but_not_reads() {
    let world = MemWorld::new();
    let id = ResponseId::new(tag());
    world
        .ledger
        .create(record(&id, "t1", true), IdempotencyKey("k".into()), 0)
        .await
        .unwrap();
    world.store.set_read_only(true);

    assert_eq!(
        world
            .ledger
            .create(record(&ResponseId::new(tag()), "t1", true), IdempotencyKey("k2".into()), 0)
            .await,
        Ok(nova_responses::CreateOutcome::ReadOnly)
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
        .create(record(&first, "t1", true), IdempotencyKey("k1".into()), 0)
        .await
        .unwrap();

    let second = ResponseId::new(tag());
    assert_eq!(
        world
            .ledger
            .create(record(&second, "t1", true), IdempotencyKey("k2".into()), 0)
            .await
            .unwrap(),
        nova_responses::CreateOutcome::Overloaded
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
            .create(rec, IdempotencyKey(format!("k-{id}")), 0)
            .await
            .unwrap();
    }
    let keep = ResponseId::new(tag());
    world
        .ledger
        .create(record(&keep, "t2", true), IdempotencyKey("keep".into()), 0)
        .await
        .unwrap();

    assert_eq!(world.ledger.delete_by_tenant(&tenant("t1")).await.unwrap(), 2);
    assert_eq!(world.conversation.delete_by_tenant(&tenant("t1")).await.unwrap(), 1);
    // The other tenant's records survive.
    assert!(world.ledger.get(&keep).await.unwrap().is_some());
    assert!(world.conversation.get(&tenant("t2"), &conv).await.unwrap().is_none());
}
