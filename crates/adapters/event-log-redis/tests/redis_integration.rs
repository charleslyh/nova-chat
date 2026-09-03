//! Integration tests against a real Redis server.
//!
//! Ignored by default: they need `redis-server` listening on `127.0.0.1:6379`.
//! Run with:
//!
//! ```text
//! redis-server --port 6379 &
//! cargo test -p adapters-event-log-redis --test redis_integration -- --ignored --nocapture
//! ```
//!
//! These exist because the Lua `INCR`+`XADD` atomicity (and the tombstone →
//! `Expired` mapping) are behaviours of the Redis server itself, which the
//! in-crate unit tests can only reason about, not exercise.

use std::sync::Arc;
use std::time::Duration;

use adapters_event_log_redis::RedisResponseEventLog;
use adapters_mem::MemWorld;
use nova_responses_core::{
    EventLogError, ResponseEvent, ResponseEventKind, ResponseEventLog, ResponseId, ResponseLedger,
};

fn id() -> ResponseId {
    ResponseId::new(nova_responses_core::NodeTag::parse("n1").unwrap())
}

/// A lifecycle event: no attempt fence, so the mem ledger's `check_attempt` is
/// never reached and the test needs no claim setup.
fn event(id: &ResponseId) -> ResponseEvent {
    ResponseEvent::lifecycle(
        id.clone(),
        ResponseEventKind::Created,
        serde_json::json!({ "id": id.to_string(), "object": "response", "status": "queued" }),
    )
}

async fn connect() -> RedisResponseEventLog {
    let world = MemWorld::new();
    // The ledger's store stays alive through the Arc held by the log, even after
    // `world` (and its other ports) drops here.
    let ledger: Arc<dyn ResponseLedger> = world.ledger.clone();
    RedisResponseEventLog::connect("redis://127.0.0.1:6379", ledger, "itest")
        .await
        .expect("redis-server on 127.0.0.1:6379 is required (see file docs)")
}

#[tokio::test]
#[ignore]
async fn append_then_read_returns_contiguous_sequence_numbers() {
    let log = connect().await;
    let id = id();

    for _ in 0..5 {
        log.append(event(&id)).await.unwrap();
    }

    let batch = log.read_after(&id, None, 10, 0).await.unwrap();
    let seqs: Vec<u64> = batch.iter().map(|e| e.sequence_number).collect();
    assert_eq!(seqs, vec![0, 1, 2, 3, 4]);

    // `starting_after` is exclusive.
    let tail = log.read_after(&id, Some(1), 10, 0).await.unwrap();
    assert_eq!(tail.first().unwrap().sequence_number, 2);
}

#[tokio::test]
#[ignore]
async fn concurrent_appends_never_lose_a_sequence_number() {
    let log = Arc::new(connect().await);
    let id = id();

    const N: usize = 200;
    let mut handles = Vec::new();
    for _ in 0..N {
        let log = log.clone();
        let id = id.clone();
        handles.push(tokio::spawn(async move {
            log.append(event(&id)).await.unwrap()
        }));
    }

    let mut seqs: Vec<u64> = Vec::new();
    for h in handles {
        seqs.push(h.await.unwrap());
    }
    seqs.sort_unstable();
    let expected: Vec<u64> = (0..N as u64).collect();
    assert_eq!(
        seqs, expected,
        "concurrent appends must assign each sequence number exactly once"
    );

    // The stream holds all of them, in order, with no gaps.
    let batch = log.read_after(&id, None, N + 1, 0).await.unwrap();
    assert_eq!(batch.len(), N);
}

#[tokio::test]
#[ignore]
async fn close_expires_to_tombstone_not_unknown() {
    let log = connect().await;
    let id = id();
    log.append(event(&id)).await.unwrap();

    // Retain 100 ms, then the tombstone outlives the stream for a further ~1 s.
    log.close(&id, 0, 100).await.unwrap();
    assert!(log.read_after(&id, None, 10, 0).await.is_ok());

    tokio::time::sleep(Duration::from_millis(300)).await;
    // Stream gone, tombstone still present → Expired, not Unknown.
    let err = log.read_after(&id, None, 10, 0).await.unwrap_err();
    assert!(
        matches!(err, EventLogError::Expired),
        "a late subscriber must see Expired, got {err:?}"
    );
}
