//! The sweeper, owned by the responses service.
//!
//! Reaping lost claims and releasing expired event buffers run on the same tick. Three
//! separate loops would mean three timers and three chances to forget one.
//!
//! The loop lives here, in the capability layer, because reaping a lost claim is a
//! response-lifecycle transition — raise the attempt fence, emit `Failed`, close the
//! stream, release the turn marker — so it belongs beside the other lifecycle
//! transitions (`create`, `cancel`), not in the assembly layer or a standalone process.

use std::time::Duration;

use tokio::sync::watch;
use tracing::{info, warn};

use crate::clock::Clock;
use crate::conversation::TurnCommit;
use crate::events::{AppendEvent, EventBody, ResponseEventKind};
use crate::ports::{
    metric, ConversationSnapshots, EventLogError, MetricsSink, ResponseEventLog, ResponseLedger,
    TurnLock,
};
use crate::protocol::{ContentPart, ItemStatus, ResponseItem, ResponseObject};
use crate::response::{ResponseId, ResponseStatus};
use crate::usage::Usage;

use super::responses::ResponsesDeps;

const TICK: Duration = Duration::from_secs(2);

/// Events per read while replaying a response's stream for its completed items.
const REPLAY_PAGE: usize = 256;

/// Spawn the background loop. Every peer runs one; reaping is idempotent, so the
/// redundant loops race harmlessly.
///
/// The loop exits when `shutdown` is closed (its sender dropped by
/// [`crate::service::ResponsesService::stop`]) or a value is sent on it — at the next
/// tick boundary, without interrupting an in-flight tick.
pub(crate) fn spawn(shutdown: watch::Receiver<()>, deps: &ResponsesDeps) {
    let ledger = deps.ledger.clone();
    let event_log = deps.event_log.clone();
    let turn_lock = deps.turn_lock.clone();
    let snapshots = deps.snapshots.clone();
    let clock = deps.clock.clone();
    let metrics = deps.metrics.clone();
    let heartbeat_ttl = deps.cfg.heartbeat_ttl();
    let retain_after_terminal = deps.cfg.retain_after_terminal();

    tokio::spawn(async move {
        let mut shutdown = shutdown;
        loop {
            tokio::select! {
                // `changed()` resolves with an error once the sender is dropped, so a
                // `stop` that drops the sender is as much a signal as an explicit send.
                _ = shutdown.changed() => break,
                _ = tokio::time::sleep(TICK) => {
                    tick(
                        ledger.as_ref(),
                        event_log.as_ref(),
                        turn_lock.as_ref(),
                        snapshots.as_ref(),
                        clock.as_ref(),
                        metrics.as_ref(),
                        heartbeat_ttl,
                        retain_after_terminal,
                    )
                    .await;
                }
            }
        }
    });
}

async fn tick(
    ledger: &dyn ResponseLedger,
    event_log: &dyn ResponseEventLog,
    turn_lock: &dyn TurnLock,
    snapshots: &dyn ConversationSnapshots,
    clock: &dyn Clock,
    metrics: &dyn MetricsSink,
    heartbeat_ttl: Duration,
    retain_after_terminal: Duration,
) {
    let now = clock.now_ms();

    // 1. Reap claims whose holder stopped heartbeating. The ledger raises the attempt
    //    fence, so the dead holder cannot append afterwards (INV-6).
    let aborted = match ledger.reap(now, heartbeat_ttl).await {
        Ok(aborted) => aborted,
        Err(e) => {
            warn!(error = %e, "sweeper reap failed");
            return;
        }
    };

    for claim in aborted {
        info!(
            response = %claim.response_id,
            tenant = %claim.tenant_id,
            conversation = ?claim.conversation_id,
            "reaping a lost claim"
        );
        // Partial usage is booked by the ledger itself during reaping, so a crash
        // between the two cannot lose it (INV-51).
        //
        // The terminal event carries a stub object: the full record could be read back
        // now that the claim carries a tenant, but that would be an extra read per
        // reaped claim to enrich an event whose only job is to end the stream.
        // Subscribers that want the finished object use `GET`.
        if let Err(e) = event_log
            .append(AppendEvent::lifecycle(
                claim.response_id.clone(),
                ResponseEventKind::Failed,
                ResponseObject::terminal_stub(&claim.response_id, ResponseStatus::Failed),
            ))
            .await
        {
            warn!(
                response = %claim.response_id,
                error = %e,
                "terminal event append for a reaped claim failed; subscribers may hang until the stream closes"
            );
        }
        if let Err(e) = event_log
            .close(&claim.response_id, now, retain_after_terminal)
            .await
        {
            warn!(
                response = %claim.response_id,
                error = %e,
                "retention window for a reaped claim not started"
            );
        }

        // Archive the reaped turn's input and whatever output it reached (D30
        // incomplete-turn archival). The holder is gone, so completed items come from
        // replaying the event stream (INV-48 RESTATE, still within the retention
        // window); a half-streamed message is reconstructed from its deltas as an
        // incomplete message.
        //
        // Ordering mirrors the engine's settle: the snapshot lands **before** the lock is
        // released, so a client that sees `turn_completed` and starts the next turn
        // cannot read a snapshot that is still missing this turn's input (§3.2).
        if claim.store {
            if let Some(conversation_id) = &claim.conversation_id {
                match replay_completed_items(event_log, &claim.response_id).await {
                    Ok(output_items) => {
                        if let Err(e) = snapshots
                            .append_turn(
                                &claim.tenant_id,
                                conversation_id,
                                &claim.response_id,
                                TurnCommit {
                                    input_items: claim.input_items.clone(),
                                    output_items,
                                    reasoning: None,
                                    usage: Usage::default(),
                                    status: ResponseStatus::Failed,
                                },
                                now,
                            )
                            .await
                        {
                            warn!(
                                response = %claim.response_id,
                                conversation = %conversation_id,
                                error = %e,
                                "could not archive a reaped turn to the conversation snapshot"
                            );
                        }
                    }
                    Err(e) => {
                        warn!(
                            response = %claim.response_id,
                            error = %e,
                            "replaying completed items for archival failed"
                        );
                    }
                }
            }
        }

        // Release the in-flight marker. `Failed` rather than a status of its own: from a
        // client's point of view a reaped turn is a failed turn, and inventing a sixth
        // status would oblige every subscriber to learn one.
        if let Some(conversation_id) = &claim.conversation_id {
            if let Err(e) = turn_lock
                .release_active(
                    &claim.tenant_id,
                    conversation_id,
                    &claim.response_id,
                    ResponseStatus::Failed,
                    now,
                )
                .await
            {
                // Not retried here: the reap already moved the record out of
                // `in_progress`, so the next tick will not select it again. The recovery
                // is on the admission side instead — a marker whose holder is already
                // terminal is taken over by the next `acquire_active`.
                warn!(
                    response = %claim.response_id,
                    conversation = %conversation_id,
                    error = %e,
                    "could not release the turn marker for a reaped claim; the next \
                     turn on this conversation will take the stale marker over"
                );
            }
        }

        metrics.incr(metric::RESPONSES_REAPED, 1);
    }

    // 2. Release event buffers past their retention window.
    match event_log.sweep_expired(now).await {
        Ok(0) => {}
        Ok(n) => metrics.incr(metric::EVENT_LOGS_SWEPT, n),
        Err(e) => warn!(error = %e, "event log sweep failed"),
    }
}

/// Replay a response's event stream for its archivable output items, in
/// `output_index` order.
///
/// This is the archival data source for terminal paths where the executing agent
/// is unreachable (cancel/reap): the agent has stopped or died, so its output
/// exists only as events. Items that reached a `done` boundary come back as they
/// completed. The item still being streamed — `added` but never `done` — is
/// reconstructed from its accumulated text deltas as a message with
/// `ItemStatus::Incomplete`, so a cancelled generation keeps the tokens the user
/// already saw (INV-61). Half-streamed function-call arguments are not
/// reconstructed: they cannot execute and would not round-trip as input.
pub(crate) async fn replay_completed_items(
    event_log: &dyn ResponseEventLog,
    response_id: &ResponseId,
) -> Result<Vec<ResponseItem>, EventLogError> {
    let mut items: Vec<(u32, ResponseItem)> = Vec::new();
    // The item currently being streamed: set on `added`, consumed on `done`.
    let mut open: Option<(u32, ResponseItem)> = None;
    let mut open_text = String::new();
    let mut cursor: Option<u64> = None;
    loop {
        let batch = event_log
            .read_after(response_id, cursor, REPLAY_PAGE, Duration::ZERO)
            .await?;
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
    // The trailing open item, if it streamed any text, is the half-finished
    // message the user was reading when the turn ended. Archive it as an
    // incomplete message rather than dropping the tokens.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::NodeTag;
    use crate::ports::EventLogError;
    use crate::protocol::Role;
    use crate::{Attempt, ResponseEvent};
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    fn id() -> ResponseId {
        ResponseId::new(NodeTag::parse("n1").unwrap())
    }

    /// Minimal in-memory log: assigns INV-11 sequence numbers per response and
    /// stores events in order. Just enough of the port for
    /// `replay_completed_items`.
    struct Log {
        id: ResponseId,
        events: Mutex<BTreeMap<ResponseId, Vec<ResponseEvent>>>,
    }

    impl Log {
        fn new() -> Self {
            Self {
                id: id(),
                events: Mutex::new(BTreeMap::new()),
            }
        }

        fn id(&self) -> ResponseId {
            self.id.clone()
        }

        fn push(&self, event: AppendEvent) {
            let mut g = self.events.lock().unwrap();
            let seq = g.get(event.response_id()).map(Vec::len).unwrap_or(0) as u64;
            g.entry(event.response_id().clone())
                .or_default()
                .push(ResponseEvent::from_parts(seq, event));
        }

        fn done(&self, index: u32, text: &str) {
            self.push(AppendEvent::item(
                self.id(),
                Attempt(1),
                true,
                index,
                ResponseItem::assistant_text(text),
            ));
        }

        fn added(&self, index: u32) {
            self.push(AppendEvent::item(
                self.id(),
                Attempt(1),
                false,
                index,
                ResponseItem::Message {
                    role: Role::Assistant,
                    content: vec![],
                    id: Some(format!("m{index}")),
                    status: Some(ItemStatus::InProgress),
                },
            ));
        }

        fn text(&self, index: u32, delta: &str) {
            self.push(AppendEvent::text_delta(
                self.id(),
                Attempt(1),
                format!("m{index}"),
                index,
                0,
                delta,
            ));
        }
    }

    #[async_trait::async_trait]
    impl ResponseEventLog for Log {
        async fn append(&self, event: AppendEvent) -> Result<u64, EventLogError> {
            let mut g = self.events.lock().unwrap();
            let seq = g.get(event.response_id()).map(Vec::len).unwrap_or(0) as u64;
            g.entry(event.response_id().clone())
                .or_default()
                .push(ResponseEvent::from_parts(seq, event.clone()));
            Ok(seq)
        }

        async fn read_after(
            &self,
            response_id: &ResponseId,
            starting_after: Option<u64>,
            limit: usize,
            _wait: Duration,
        ) -> Result<Vec<ResponseEvent>, EventLogError> {
            let g = self.events.lock().unwrap();
            let start = starting_after.map(|s| (s + 1) as usize).unwrap_or(0);
            Ok(g.get(response_id)
                .map(|v| v.iter().skip(start).take(limit).cloned().collect())
                .unwrap_or_default())
        }

        async fn close(
            &self,
            _response_id: &ResponseId,
            _now_ms: u64,
            _retain: Duration,
        ) -> Result<(), EventLogError> {
            Ok(())
        }

        async fn sweep_expired(&self, _now_ms: u64) -> Result<u64, EventLogError> {
            Ok(0)
        }

        async fn remove(&self, response_id: &ResponseId) -> Result<(), EventLogError> {
            self.events.lock().unwrap().remove(response_id);
            Ok(())
        }
    }

    fn message_text(item: &ResponseItem) -> String {
        match item {
            ResponseItem::Message { content, .. } => content
                .iter()
                .map(|p| match p {
                    ContentPart::OutputText { text } => text.clone(),
                    _ => String::new(),
                })
                .collect(),
            _ => String::new(),
        }
    }

    #[tokio::test]
    async fn done_items_come_back_in_order() {
        let log = Log::new();
        log.done(0, "first");
        log.done(1, "second");
        let items = replay_completed_items(&log, &log.id()).await.unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(message_text(&items[0]), "first");
        assert_eq!(message_text(&items[1]), "second");
    }

    #[tokio::test]
    async fn an_open_item_with_deltas_is_reconstructed_as_incomplete() {
        let log = Log::new();
        log.done(0, "done first");
        log.added(1);
        log.text(1, "half ");
        log.text(1, "streamed");
        let items = replay_completed_items(&log, &log.id()).await.unwrap();
        assert_eq!(items.len(), 2, "completed + reconstructed: {items:?}");
        assert_eq!(message_text(&items[0]), "done first");
        match &items[1] {
            ResponseItem::Message { status, .. } => {
                assert_eq!(status, &Some(ItemStatus::Incomplete));
            }
            other => panic!("expected a message, got {other:?}"),
        }
        assert_eq!(message_text(&items[1]), "half streamed");
    }

    #[tokio::test]
    async fn an_open_item_without_deltas_is_dropped() {
        let log = Log::new();
        log.added(0);
        let items = replay_completed_items(&log, &log.id()).await.unwrap();
        assert!(items.is_empty(), "no done items, no deltas: {items:?}");
    }

    #[tokio::test]
    async fn a_done_item_closes_the_open_item_window() {
        // Deltas landing after the item's `done` belong to nothing and must not
        // resurrect a partial message.
        let log = Log::new();
        log.added(0);
        log.text(0, "complete");
        log.done(0, "complete");
        log.text(0, "late delta");
        let items = replay_completed_items(&log, &log.id()).await.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(message_text(&items[0]), "complete");
    }

    #[tokio::test]
    async fn reasoning_deltas_are_not_output_text() {
        let log = Log::new();
        log.added(0);
        log.push(AppendEvent::reasoning_text_delta(
            log.id(),
            Attempt(1),
            "thinking",
        ));
        log.text(0, "answer");
        let items = replay_completed_items(&log, &log.id()).await.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(message_text(&items[0]), "answer");
    }

    #[tokio::test]
    async fn half_streamed_function_call_arguments_are_not_reconstructed() {
        let log = Log::new();
        log.push(AppendEvent::item(
            log.id(),
            Attempt(1),
            false,
            0,
            ResponseItem::FunctionCall {
                call_id: "call_1".into(),
                name: "search".into(),
                arguments: String::new(),
                id: None,
                status: Some(ItemStatus::InProgress),
            },
        ));
        log.push(AppendEvent::arguments_delta(
            log.id(),
            Attempt(1),
            "call_1",
            0,
            "{\"q\":",
        ));
        let items = replay_completed_items(&log, &log.id()).await.unwrap();
        assert!(
            items.is_empty(),
            "half arguments cannot execute and must not be archived: {items:?}"
        );
    }
}
