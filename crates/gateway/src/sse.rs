//! SSE plumbing.
//!
//! Shape: probe first, then stream. The probe exists so an unknown id or an
//! expired cursor becomes a proper HTTP status (404 / 410) — once the SSE body
//! has started, the status is already committed and the only way to report a
//! problem would be an in-band error event, which clients routinely ignore.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use nova_responses_core::{EventLogError, ResponseEventLog, ResponseId};

use crate::error::api_error;

/// How long a single read may block waiting for new events. Long enough to keep
/// first-token latency low, short enough that keep-alives still flow.
const READ_WAIT_MS: u64 = 500;

/// Events per read. Batching cuts wake-ups on fast streams.
const BATCH: usize = 64;

pub fn map_event_log_error(err: &EventLogError) -> (StatusCode, &'static str, String) {
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
        EventLogError::ReadOnly => (
            StatusCode::SERVICE_UNAVAILABLE,
            "read_only",
            "service is read-only".into(),
        ),
        EventLogError::CapacityExceeded => (
            StatusCode::SERVICE_UNAVAILABLE,
            "capacity_exceeded",
            "event buffer capacity exceeded".into(),
        ),
        EventLogError::Internal(msg) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            msg.clone(),
        ),
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
    // Probe: surface unknown/expired as a status code before committing to 200.
    if let Err(e) = event_log
        .read_after(&response_id, starting_after, 1, 0)
        .await
    {
        let (status, code, message) = map_event_log_error(&e);
        return api_error(status, code, message);
    }

    let stream = futures::stream::unfold(
        (event_log, response_id, starting_after, false),
        move |(log, id, cursor, finished)| async move {
            if finished {
                return None;
            }
            loop {
                match log.read_after(&id, cursor, BATCH, READ_WAIT_MS).await {
                    Ok(batch) if !batch.is_empty() => {
                        let last = batch.last().map(|e| e.sequence_number);
                        let terminal = batch.iter().any(|e| e.kind.is_terminal());
                        let events: Vec<Result<Event, Infallible>> = batch
                            .iter()
                            .map(|ev| {
                                let data = serde_json::to_string(ev).unwrap_or_default();
                                // `.id()` feeds Last-Event-ID so a reconnect can
                                // resume without the client tracking state
                                // itself; `.event()` carries the protocol event
                                // name.
                                Ok(Event::default()
                                    .event(ev.kind.as_str())
                                    .id(ev.sequence_number.to_string())
                                    .data(data))
                            })
                            .collect();
                        return Some((
                            futures::stream::iter(events),
                            (log, id, last.or(cursor), terminal),
                        ));
                    }
                    Ok(_) => {
                        // Long poll expired with nothing new; loop and wait
                        // again. Keep-alive frames prevent idle disconnects.
                        continue;
                    }
                    Err(e) => {
                        // Mid-stream the status is already sent, so the failure
                        // has to be reported in band. It is still explicit: no
                        // partial data is invented and the stream ends here.
                        let (_, code, message) = map_event_log_error(&e);
                        let payload = serde_json::json!({
                            "type": "error",
                            "error": { "code": code, "message": message },
                        })
                        .to_string();
                        let ev = Event::default().event("error").data(payload);
                        return Some((
                            futures::stream::iter(vec![Ok(ev)]),
                            (log, id, cursor, true),
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
}
