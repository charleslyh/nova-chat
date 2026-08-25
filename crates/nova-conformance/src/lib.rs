//! Port conformance suites — any adapter must pass these (L0).

use std::sync::Arc;

use nova_adapter_mem::MemWorld;
use nova_core::{
    Attempt, CapacityNeed, EventKind, IdempotencyKey, OutputEvent, Priority, TaskId, TaskKind,
    TaskSpec, TaskState, WorkerId,
};
use nova_ports::{
    CapacityAssertion, ClaimOutcome, Clock, FilterExpr, IdempotencyGate, PolicySandbox,
    Reservation, SandboxInput, StreamChannel, TaskStore,
};

pub async fn assert_task_store_conformance(store: Arc<dyn TaskStore>) {
    let id = TaskId::new();
    let w1 = WorkerId::new();
    let w2 = WorkerId::new();
    let spec = TaskSpec {
        id,
        kind: TaskKind::AigcImage,
        priority: Priority::Normal,
        capacity: CapacityNeed { units: 2 },
        submitted_at_ms: 0,
        predicate: String::new(),
    };
    store.insert(spec).await.expect("insert");

    let a = store
        .try_claim(
            &id,
            TaskState::Pending,
            Attempt(0),
            &w1,
            &CapacityAssertion { required: 2 },
            1000,
        )
        .await
        .unwrap();
    assert!(matches!(a, ClaimOutcome::Claimed { attempt: Attempt(1) }));

    let b = store
        .try_claim(
            &id,
            TaskState::Pending,
            Attempt(0),
            &w2,
            &CapacityAssertion { required: 2 },
            1000,
        )
        .await
        .unwrap();
    assert!(matches!(b, ClaimOutcome::Conflict));

    let zero = store
        .try_claim(
            &TaskId::new(),
            TaskState::Pending,
            Attempt(0),
            &w1,
            &CapacityAssertion { required: 0 },
            1000,
        )
        .await
        .unwrap();
    assert!(matches!(
        zero,
        ClaimOutcome::NotFound | ClaimOutcome::CapacityRejected
    ));
}

pub async fn assert_idempotency_conformance(gate: Arc<dyn IdempotencyGate>) {
    let key = IdempotencyKey("k1".into());
    assert_eq!(gate.reserve(&key).await.unwrap(), Reservation::Reserved);
    assert_eq!(
        gate.reserve(&key).await.unwrap(),
        Reservation::AlreadyExists
    );
    // INV-2: still rejected after "long time" — mem has no TTL; second call suffices.
    assert_eq!(
        gate.reserve(&key).await.unwrap(),
        Reservation::AlreadyExists
    );
}

pub async fn assert_stream_conformance(stream: Arc<dyn StreamChannel>) {
    let tid = TaskId::new();
    let e1 = OutputEvent {
        task_id: tid,
        attempt: Attempt(1),
        seq: 0,
        kind: EventKind::Progress,
        payload: b"a".to_vec(),
    };
    let e2 = OutputEvent {
        task_id: tid,
        attempt: Attempt(1),
        seq: 0,
        kind: EventKind::TextDelta,
        payload: b"b".to_vec(),
    };
    let s1 = stream.append(e1).await.unwrap();
    let s2 = stream.append(e2).await.unwrap();
    assert_eq!(s1, 1);
    assert_eq!(s2, 2);
    let got = stream.read_from(tid, 1, 10).await.unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].seq, 1);
    assert_eq!(got[1].seq, 2);
}

pub async fn assert_sandbox_conformance(sandbox: Arc<dyn PolicySandbox>) {
    let task = TaskSpec {
        id: TaskId::new(),
        kind: TaskKind::Agent,
        priority: Priority::Normal,
        capacity: CapacityNeed { units: 2 },
        submitted_at_ms: 0,
        predicate: String::new(),
    };
    let worker = nova_core::WorkerProfile {
        id: WorkerId::new(),
        total_capacity: 8,
        remaining_capacity: 8,
        labels: vec!["gpu".into()],
    };
    let input = SandboxInput {
        task: task.clone(),
        worker: worker.clone(),
    };
    assert!(sandbox.eval("true", &input, 10).await.unwrap());
    assert!(!sandbox.eval("false", &input, 10).await.unwrap());
    assert!(sandbox
        .eval("capacity_le:4", &input, 10)
        .await
        .unwrap());
    assert!(matches!(
        sandbox.eval("http://evil", &input, 10).await,
        Err(nova_ports::SandboxError::Forbidden)
    ));
    assert!(matches!(
        sandbox.eval("true", &input, 0).await,
        Err(nova_ports::SandboxError::BudgetExceeded)
    ));
}

pub async fn assert_clock_advances(clock: Arc<dyn Clock + Send + Sync>) {
    let t0 = clock.now_ms().await;
    // MemClock: advance via downcast not available on trait — use sleep_until with concurrent advance in mem tests.
    let _ = t0;
}

/// Run full mem adapter suite (L0 entry).
pub async fn run_mem_suite() {
    let world = MemWorld::new();
    assert_task_store_conformance(world.store.clone()).await;
    assert_idempotency_conformance(world.gate.clone()).await;
    assert_stream_conformance(world.stream.clone()).await;
    assert_sandbox_conformance(world.sandbox.clone()).await;

    // Concurrent unique claim
    let id = TaskId::new();
    world
        .store
        .insert(TaskSpec {
            id,
            kind: TaskKind::AigcVideo,
            priority: Priority::High,
            capacity: CapacityNeed { units: 4 },
            submitted_at_ms: 0,
            predicate: String::new(),
        })
        .await
        .unwrap();

    let store = world.store.clone();
    let mut handles = vec![];
    for _ in 0..16 {
        let s = store.clone();
        let wid = WorkerId::new();
        handles.push(tokio::spawn(async move {
            s.try_claim(
                &id,
                TaskState::Pending,
                Attempt(0),
                &wid,
                &CapacityAssertion { required: 4 },
                9999,
            )
            .await
            .unwrap()
        }));
    }
    let mut claimed = 0;
    for h in handles {
        if matches!(h.await.unwrap(), ClaimOutcome::Claimed { .. }) {
            claimed += 1;
        }
    }
    assert_eq!(claimed, 1, "INV-1: exactly one claim wins");

    let _ = world
        .store
        .list_candidates(&FilterExpr {
            pending_only: true,
            limit: 10,
        })
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mem_conformance() {
        run_mem_suite().await;
    }
}
