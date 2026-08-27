//! L0 protocol conformance — Session stream stack.

use std::sync::Arc;

use adapters_mem::MemWorld;
use nova_sessions_core::{
    AgentId, Attempt, EventKind, IdempotencyKey, MetaStore, SessionId, SessionSnapshot,
    SnapshotStore, StreamChannel, StreamError, StreamEvent, SubmitOutcome, TurnId, TurnStatus,
};

pub async fn assert_stream_conformance(stream: Arc<dyn StreamChannel>, session: SessionId) {
    let seq1 = stream
        .append(StreamEvent {
            session_id: session,
            seq: 0,
            kind: EventKind::TurnBegin,
            turn_id: Some(TurnId::new()),
            attempt: None,
            payload: "hi".into(),
        })
        .await
        .expect("append");
    assert_eq!(seq1, 1);

    let batch = stream.read_from(session, 1, 10).await.expect("read");
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].seq, 1);

    let empty = stream.read_from(session, 100, 10).await.expect("future");
    assert!(empty.is_empty());
}

pub async fn assert_snapshot_conformance(store: Arc<dyn SnapshotStore>) {
    let sid = SessionId::new();
    let s1 = SessionSnapshot {
        session_id: sid,
        snapshot_seq: 1,
        bubbles: vec![],
        running: vec![],
    };
    store.put(s1.clone()).await.expect("put");
    let got = store.get(sid).await.expect("get").expect("some");
    assert_eq!(got.snapshot_seq, 1);

    let stale = SessionSnapshot {
        session_id: sid,
        snapshot_seq: 0,
        bubbles: vec![],
        running: vec![],
    };
    assert!(store.put(stale).await.is_err());
}

pub async fn assert_meta_conformance(meta: Arc<dyn MetaStore>) {
    let sid = meta.create_session().await.expect("session");
    let out = meta
        .submit_turn(sid, "hello".into(), IdempotencyKey("k1".into()), 0)
        .await
        .expect("submit");
    let turn_id = match out {
        SubmitOutcome::Accepted { turn_id } => turn_id,
        other => panic!("expected accepted, got {other:?}"),
    };
    let dup = meta
        .submit_turn(sid, "hello".into(), IdempotencyKey("k1".into()), 0)
        .await
        .expect("dup");
    assert!(matches!(dup, SubmitOutcome::Duplicate { turn_id: t } if t == turn_id));

    let busy = meta
        .submit_turn(sid, "again".into(), IdempotencyKey("k2".into()), 0)
        .await
        .expect("busy");
    assert_eq!(busy, SubmitOutcome::Busy);

    let agent = AgentId::new();
    let claimed = meta
        .claim_turn(agent, 1000, 60_000)
        .await
        .expect("claim")
        .expect("some");
    assert_eq!(claimed.turn.turn_id, turn_id);
    assert_eq!(claimed.attempt, Attempt(1));

    meta.complete_turn(turn_id, Attempt(1), TurnStatus::Done)
        .await
        .expect("complete");
    assert!(meta
        .complete_turn(turn_id, Attempt(1), TurnStatus::Done)
        .await
        .is_err());
}

pub async fn assert_fence_conformance(world: &MemWorld) {
    let sid = world.meta.create_session().await.unwrap();
    let turn_id = match world
        .meta
        .submit_turn(sid, "x".into(), IdempotencyKey("f1".into()), 0)
        .await
        .unwrap()
    {
        SubmitOutcome::Accepted { turn_id } => turn_id,
        _ => panic!("accept"),
    };
    let agent = AgentId::new();
    let c = world
        .meta
        .claim_turn(agent, 1_000, 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(c.attempt, Attempt(1));

    world
        .stream
        .append(StreamEvent {
            session_id: sid,
            seq: 0,
            kind: EventKind::TextDelta,
            turn_id: Some(turn_id),
            attempt: Some(Attempt(1)),
            payload: "a".into(),
        })
        .await
        .unwrap();

    let aborted = world.meta.reap(2_000, 90_000).await.unwrap();
    assert_eq!(aborted.len(), 1);

    let err = world
        .stream
        .append(StreamEvent {
            session_id: sid,
            seq: 0,
            kind: EventKind::TextDelta,
            turn_id: Some(turn_id),
            attempt: Some(Attempt(1)),
            payload: "zombie".into(),
        })
        .await;
    assert!(matches!(err, Err(StreamError::StaleAttempt)));
}

/// INV-16: TextDelta may coalesce; terminal / structural events must not.
pub fn assert_event_coalescing() {
    assert!(EventKind::TextDelta.coalescible());
    assert!(!EventKind::TurnBegin.coalescible());
    assert!(!EventKind::TurnDone.coalescible());
    assert!(!EventKind::SessionBusy.coalescible());
    assert!(!EventKind::AttemptStarted.coalescible());
}

pub async fn run_mem_suite() {
    let world = MemWorld::new();
    let sid = world.meta.create_session().await.unwrap();
    assert_stream_conformance(world.stream.clone() as Arc<dyn StreamChannel>, sid).await;
    assert_snapshot_conformance(world.snapshot.clone() as Arc<dyn SnapshotStore>).await;
    assert_meta_conformance(world.meta.clone() as Arc<dyn MetaStore>).await;
    assert_fence_conformance(&world).await;
    assert_event_coalescing();
}

/// Same as [`run_mem_suite`], printing each case name as it runs (for `just verify l0`).
pub async fn run_mem_suite_reported() {
    let world = MemWorld::new();
    let sid = world.meta.create_session().await.unwrap();

    eprint!("  stream ... ");
    assert_stream_conformance(world.stream.clone() as Arc<dyn StreamChannel>, sid).await;
    eprintln!("ok");

    eprint!("  snapshot ... ");
    assert_snapshot_conformance(world.snapshot.clone() as Arc<dyn SnapshotStore>).await;
    eprintln!("ok");

    eprint!("  meta ... ");
    assert_meta_conformance(world.meta.clone() as Arc<dyn MetaStore>).await;
    eprintln!("ok");

    eprint!("  fence ... ");
    assert_fence_conformance(&world).await;
    eprintln!("ok");

    eprint!("  event-coalesce ... ");
    assert_event_coalescing();
    eprintln!("ok");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mem_suite() {
        run_mem_suite().await;
    }
}
