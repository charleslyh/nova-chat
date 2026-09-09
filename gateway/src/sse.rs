//! SSE plumbing.
//!
//! Shape: probe first, then stream. The probe exists so an unknown id or an
//! expired cursor becomes a proper HTTP status (404 / 410) — once the SSE body
//! has started, the status is already committed and the only way to report a
//! problem would be an in-band error event, which clients routinely ignore.
//!
//! Two streams use this: the per-response event log and the conversation event
//! stream. They differ in what they read, how an event is named, and whether the
//! stream can end at all — but not in the probe, the cursor discipline, the
//! keep-alive interval or the in-band error report. Those are the parts that are
//! easy to get subtly wrong, so they exist once, behind [`SseSource`], rather
//! than twice.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures::future::BoxFuture;
use futures::StreamExt;
use nova_responses::{
    ConversationEvent, ConversationId, ConversationStore, EventLogError, ResponseEvent,
    StoreError,
    ResponseEventLog, ResponseId, TenantId,
};

use crate::error::api_error;

/// How long a single read may block waiting for new events. Long enough to keep
/// first-token latency low, short enough that keep-alives still flow.
const READ_WAIT_MS: u64 = 500;

/// Events per read on the per-response stream. Batching cuts wake-ups on fast
/// streams, and this one is the fast stream — thousands of deltas per turn.
const RESPONSE_BATCH: usize = 64;

/// A port error already translated to its HTTP form.
///
/// Normalising here means the streaming skeleton never has to know which port it
/// is serving, and the same translation serves both the probe (as a status) and a
/// mid-stream failure (as an in-band event).
pub type StreamFailure = (StatusCode, &'static str, String);

pub fn map_event_log_error(err: &EventLogError) -> StreamFailure {
    match err {
        // Unknown and expired are both explicit, and deliberately distinct:
        // expired is permanent with no recovery path (INV-40), whereas unknown
        // may simply be a wrong id.
        EventLogError::Unknown => (
            StatusCode::NOT_FOUND,
            "not_found",
            "no such response".into(),
        ),
        EventLogError::Expired => (
            StatusCode::GONE,
            "cursor_expired",
            "requested position is no longer buffered; there is no recovery path".into(),
        ),
        EventLogError::StaleAttempt => (
            StatusCode::CONFLICT,
            "stale_attempt",
            "attempt superseded".into(),
        ),
        EventLogError::Store(StoreError::ReadOnly) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "read_only",
            "service is read-only".into(),
        ),
        EventLogError::Store(StoreError::Unavailable) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "event buffer is unavailable".into(),
        ),
        EventLogError::CapacityExceeded => (
            StatusCode::SERVICE_UNAVAILABLE,
            "capacity_exceeded",
            "event buffer capacity exceeded".into(),
        ),
        EventLogError::Store(StoreError::Internal(msg)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            msg.clone(),
        ),
    }
}

/// One readable, resumable event stream.
///
/// Implementors supply only what actually differs between streams. Note the
/// absence of anything about waiting, batching or reconnection: those belong to
/// the skeleton, and an implementor that could influence them would be able to
/// break resumption for its stream alone.
pub trait SseSource: Send + Sync + 'static {
    type Event: Send;

    /// Read events strictly after `cursor`, blocking up to `wait_ms`.
    fn read(
        &self,
        cursor: Option<u64>,
        limit: usize,
        wait_ms: u64,
    ) -> BoxFuture<'_, Result<Vec<Self::Event>, StreamFailure>>;

    /// Sequence number, used as the SSE `id` so `Last-Event-ID` resumption works
    /// without the client tracking state itself.
    fn seq(event: &Self::Event) -> u64;

    /// SSE event name.
    fn name(event: &Self::Event) -> &'static str;

    /// Serialised event body.
    fn data(event: &Self::Event) -> String;

    /// Whether this event ends the stream.
    ///
    /// A per-response stream ends at a terminal status. A conversation stream
    /// never does — it is open for as long as the client stays connected, which
    /// is what makes "replay history then continue live" a single request.
    fn is_terminal(event: &Self::Event) -> bool;
}

/// Probe, then stream, for any [`SseSource`].
///
/// `batch` bounds one read, and therefore the memory a replay-from-zero holds at
/// once; the stream simply continues from where the batch ended, so it never
/// bounds how much history is reachable.
pub async fn open_sse<S: SseSource>(
    source: S,
    starting_after: Option<u64>,
    batch: usize,
) -> Response {
    // Probe: surface unknown/expired as a status code before committing to 200.
    if let Err((status, code, message)) = source.read(starting_after, 1, 0).await {
        return api_error(status, code, message);
    }

    let batch = batch.max(1);
    let stream = futures::stream::unfold(
        (Arc::new(source), starting_after, false),
        move |(source, cursor, finished)| async move {
            if finished {
                return None;
            }
            loop {
                match source.read(cursor, batch, READ_WAIT_MS).await {
                    Ok(batch) if !batch.is_empty() => {
                        let last = batch.last().map(S::seq);
                        let terminal = batch.iter().any(S::is_terminal);
                        let events: Vec<Result<Event, Infallible>> = batch
                            .iter()
                            .map(|ev| {
                                Ok(Event::default()
                                    .event(S::name(ev))
                                    .id(S::seq(ev).to_string())
                                    .data(S::data(ev)))
                            })
                            .collect();
                        return Some((
                            futures::stream::iter(events),
                            (source, last.or(cursor), terminal),
                        ));
                    }
                    Ok(_) => {
                        // Long poll expired with nothing new; loop and wait
                        // again. Keep-alive frames prevent idle disconnects.
                        continue;
                    }
                    Err((_, code, message)) => {
                        // Mid-stream the status is already sent, so the failure
                        // has to be reported in band. It is still explicit: no
                        // partial data is invented and the stream ends here.
                        let payload = serde_json::json!({
                            "type": "error",
                            "error": { "code": code, "message": message },
                        })
                        .to_string();
                        let ev = Event::default().event("error").data(payload);
                        return Some((
                            futures::stream::iter(vec![Ok(ev)]),
                            (source, cursor, true),
                        ));
                    }
                }
            }
        },
    )
    .flatten();

    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

/// The per-response token stream.
struct ResponseSource {
    event_log: Arc<dyn ResponseEventLog>,
    response_id: ResponseId,
}

impl SseSource for ResponseSource {
    type Event = ResponseEvent;

    fn read(
        &self,
        cursor: Option<u64>,
        limit: usize,
        wait_ms: u64,
    ) -> BoxFuture<'_, Result<Vec<Self::Event>, StreamFailure>> {
        Box::pin(async move {
            self.event_log
                .read_after(&self.response_id, cursor, limit, wait_ms)
                .await
                .map_err(|e| map_event_log_error(&e))
        })
    }

    fn seq(event: &Self::Event) -> u64 {
        event.sequence_number
    }

    fn name(event: &Self::Event) -> &'static str {
        event.kind.as_str()
    }

    fn data(event: &Self::Event) -> String {
        serde_json::to_string(event).unwrap_or_default()
    }

    fn is_terminal(event: &Self::Event) -> bool {
        event.kind.is_terminal()
    }
}

/// Open an SSE stream for one response.
///
/// `starting_after` is exclusive; `None` starts from sequence 0.
pub async fn open_stream(
    event_log: Arc<dyn ResponseEventLog>,
    response_id: ResponseId,
    starting_after: Option<u64>,
) -> Response {
    open_sse(
        ResponseSource {
            event_log,
            response_id,
        },
        starting_after,
        RESPONSE_BATCH,
    )
    .await
}

/// The conversation event stream (D28).
struct ConversationSource {
    conversations: Arc<dyn ConversationStore>,
    tenant: TenantId,
    conversation_id: ConversationId,
}

impl SseSource for ConversationSource {
    type Event = ConversationEvent;

    fn read(
        &self,
        cursor: Option<u64>,
        limit: usize,
        wait_ms: u64,
    ) -> BoxFuture<'_, Result<Vec<Self::Event>, StreamFailure>> {
        Box::pin(async move {
            self.conversations
                .read_after(&self.tenant, &self.conversation_id, cursor, limit, wait_ms)
                .await
                .map_err(|e| crate::error::map_conversation_stream_error(&e))
        })
    }

    fn seq(event: &Self::Event) -> u64 {
        event.seq
    }

    fn name(event: &Self::Event) -> &'static str {
        event.kind.as_str()
    }

    fn data(event: &Self::Event) -> String {
        serde_json::to_string(event).unwrap_or_default()
    }

    /// Never. A conversation outlives any single turn, so its stream has no last
    /// event: a subscriber that has caught up waits for the next one rather than
    /// being disconnected and made to reconnect.
    fn is_terminal(_event: &Self::Event) -> bool {
        false
    }
}

/// Open an SSE stream for one conversation's events.
///
/// `starting_after` is exclusive; `None` starts from sequence 0, which replays the
/// whole durable history before continuing live — that is the single call that
/// restores a reopened page, so there is no snapshot endpoint and therefore no
/// snapshot that can go stale.
pub async fn open_conversation_stream(
    conversations: Arc<dyn ConversationStore>,
    tenant: TenantId,
    conversation_id: ConversationId,
    starting_after: Option<u64>,
    batch: usize,
) -> Response {
    open_sse(
        ConversationSource {
            conversations,
            tenant,
            conversation_id,
        },
        starting_after,
        batch,
    )
    .await
}

/// Parse `starting_after` from the query or from `Last-Event-ID`.
///
/// Both are exclusive cursors. The header wins because it reflects what the
/// client actually received, whereas the query string is whatever it was told to
/// use when the connection was first opened.
pub fn resolve_cursor(query: Option<u64>, last_event_id: Option<&str>) -> Option<u64> {
    last_event_id
        .and_then(|v| v.trim().parse::<u64>().ok())
        .or(query)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nova_responses::{ConversationEventKind, ResponseStatus};

    #[test]
    fn last_event_id_takes_precedence() {
        assert_eq!(resolve_cursor(Some(5), Some("9")), Some(9));
        assert_eq!(resolve_cursor(Some(5), None), Some(5));
        assert_eq!(resolve_cursor(None, Some("0")), Some(0));
        assert_eq!(resolve_cursor(None, None), None);
    }

    #[test]
    fn malformed_last_event_id_falls_back_to_query() {
        assert_eq!(resolve_cursor(Some(3), Some("not-a-number")), Some(3));
        assert_eq!(resolve_cursor(None, Some("")), None);
    }

    #[test]
    fn zero_cursor_is_distinct_from_absent() {
        // Sequence 0 is a real event, so `Some(0)` must mean "skip event 0" and
        // `None` must mean "from the start".
        assert_eq!(resolve_cursor(Some(0), None), Some(0));
        assert_ne!(resolve_cursor(Some(0), None), None);
    }

    #[test]
    fn expiry_maps_to_gone_and_unknown_to_not_found() {
        assert_eq!(
            map_event_log_error(&EventLogError::Expired).0,
            StatusCode::GONE
        );
        assert_eq!(
            map_event_log_error(&EventLogError::Unknown).0,
            StatusCode::NOT_FOUND
        );
    }

    fn conversation_event(kind: ConversationEventKind) -> ConversationEvent {
        ConversationEvent {
            conversation_id: ConversationId::new(),
            seq: 3,
            kind,
            ts_ms: 1,
        }
    }

    #[test]
    fn a_conversation_stream_never_ends_on_its_own() {
        // Not even at a turn boundary: the conversation outlives the turn, and
        // disconnecting a caught-up subscriber would force a reconnect for every
        // turn, which is exactly the round trip the long poll removes.
        let response_id = nova_responses::ResponseId::new(
            nova_responses::NodeTag::parse("n1").unwrap(),
        );
        for kind in [
            ConversationEventKind::TurnStarted {
                response_id: response_id.clone(),
            },
            ConversationEventKind::TurnCompleted {
                response_id: response_id.clone(),
                status: ResponseStatus::Completed,
            },
            ConversationEventKind::TurnCompleted {
                response_id,
                status: ResponseStatus::Cancelled,
            },
        ] {
            assert!(!ConversationSource::is_terminal(&conversation_event(kind)));
        }
    }

    #[test]
    fn both_sources_use_the_sequence_number_as_the_sse_id() {
        // Resumption depends on this: `Last-Event-ID` is fed back as
        // `starting_after`, so the id must be the cursor and nothing else.
        let ev = conversation_event(ConversationEventKind::TurnStarted {
            response_id: nova_responses::ResponseId::new(
                nova_responses::NodeTag::parse("n1").unwrap(),
            ),
        });
        assert_eq!(ConversationSource::seq(&ev), ev.seq);
        assert_eq!(ConversationSource::name(&ev), "conversation.turn_started");
        assert!(ConversationSource::data(&ev).contains("conversation.turn_started"));
    }
}
