//! Error shaping.
//!
//! One rule drives the whole mapping: **every failure gets a distinct, explicit
//! status**. Nothing degrades to a partial success, and nothing that is really a
//! failure returns 200 (INV-43).

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use nova_responses_core::{ContextError, ConversationError, LedgerError, SessionError};

pub fn api_error(status: StatusCode, code: &str, message: impl Into<String>) -> Response {
    (
        status,
        Json(serde_json::json!({
            "error": {
                "type": code,
                "message": message.into(),
            }
        })),
    )
        .into_response()
}

pub fn bad_request(code: &str, message: impl Into<String>) -> Response {
    api_error(StatusCode::BAD_REQUEST, code, message)
}

/// Ownership failures and genuine absences look identical from outside, so ids
/// cannot be enumerated (SEC-2).
pub fn not_found() -> Response {
    api_error(StatusCode::NOT_FOUND, "not_found", "no such response")
}

/// The same rule for a named resource kind.
///
/// The wording differs but the status does not, and it carries no hint about
/// whether the id exists under another tenant.
pub fn not_found_kind(kind: &str) -> Response {
    api_error(
        StatusCode::NOT_FOUND,
        "not_found",
        format!("no such {kind}"),
    )
}

pub fn map_context_error(err: &ContextError) -> Response {
    match err {
        ContextError::NotFound => not_found(),
        // Cross-tenant is reported as absent for the same reason.
        ContextError::CrossTenant => not_found(),
        ContextError::NotStored => bad_request(
            "previous_not_stored",
            "the referenced response was created with store=false and cannot be chained",
        ),
        ContextError::ChainBroken(id) => bad_request(
            "chain_broken",
            format!("chain link {id} is missing or expired; resend the history explicitly"),
        ),
        ContextError::ChainTooLong { limit } => bad_request(
            "chain_too_long",
            format!("chain exceeds the {limit} link limit; start a new chain"),
        ),
        ContextError::ChainTooLarge { limit } => bad_request(
            "chain_too_large",
            format!("chain exceeds {limit} bytes; start a new chain"),
        ),
        ContextError::IntegrityMismatch => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "integrity_mismatch",
            "stored content failed its integrity check",
        ),
        ContextError::CapacityExceeded => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "capacity_exceeded",
            "storage capacity exceeded",
        ),
        // Refusing the write is the point: storing nothing while reporting
        // success would break the chain on some later turn (INV-46).
        ContextError::Unavailable => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "store_unavailable",
            "context store is unavailable; the request was not stored",
        ),
        ContextError::ReadOnly => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "read_only",
            "service is read-only",
        ),
        ContextError::Internal(msg) => {
            api_error(StatusCode::INTERNAL_SERVER_ERROR, "internal", msg.clone())
        }
    }
}

pub fn map_ledger_error(err: &LedgerError) -> Response {
    match err {
        LedgerError::NotFound => not_found(),
        LedgerError::StaleAttempt => api_error(
            StatusCode::CONFLICT,
            "stale_attempt",
            "attempt superseded by a newer one",
        ),
        LedgerError::InvalidTransition(msg) => {
            api_error(StatusCode::CONFLICT, "invalid_transition", msg.clone())
        }
        LedgerError::ReadOnly => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "read_only",
            "service is read-only",
        ),
        LedgerError::Unavailable => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "ledger is unavailable",
        ),
        LedgerError::Internal(msg) => {
            api_error(StatusCode::INTERNAL_SERVER_ERROR, "internal", msg.clone())
        }
    }
}

/// Session errors in their streaming form: status, code and message, so the SSE
/// skeleton can use one translation for both the probe and a mid-stream failure.
pub fn map_session_stream_error(err: &SessionError) -> (StatusCode, &'static str, String) {
    match err {
        SessionError::NotFound => (
            StatusCode::NOT_FOUND,
            "not_found",
            "no such session".into(),
        ),
        // 409, matching how a superseded attempt is reported: the caller asked for
        // something the current state does not allow, and can act on that.
        // The holder is named in the message: the caller can subscribe to that
        // response's stream and watch the turn it collided with, instead of
        // polling to find out when the session frees up.
        SessionError::Busy { holder } => (
            StatusCode::CONFLICT,
            "session_busy",
            format!("a turn is already in flight for this session: {holder}"),
        ),
        // Also 409, and also on purpose: the caller asked to bind a conversation
        // that is already owned. Reporting 404 would be a lie, and 400 would
        // suggest the request was malformed when it was merely late.
        SessionError::ConversationTaken => (
            StatusCode::CONFLICT,
            "conversation_taken",
            "the conversation already belongs to another session".into(),
        ),
        SessionError::CapacityExceeded => (
            StatusCode::SERVICE_UNAVAILABLE,
            "capacity_exceeded",
            "session event capacity exceeded".into(),
        ),
        SessionError::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "store_unavailable",
            "session store is unavailable; the request was not stored".into(),
        ),
        SessionError::ReadOnly => (
            StatusCode::SERVICE_UNAVAILABLE,
            "read_only",
            "service is read-only".into(),
        ),
        SessionError::Internal(msg) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            msg.clone(),
        ),
    }
}

pub fn map_session_error(err: &SessionError) -> Response {
    let (status, code, message) = map_session_stream_error(err);
    api_error(status, code, message)
}

pub fn map_conversation_error(err: &ConversationError) -> Response {
    match err {
        ConversationError::NotFound => not_found_kind("conversation"),
        ConversationError::CapacityExceeded => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "capacity_exceeded",
            "storage capacity exceeded",
        ),
        // Refusing the write is the point: reporting success while storing nothing
        // would break the chain on some later turn (INV-46).
        ConversationError::Unavailable => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "store_unavailable",
            "conversation store is unavailable; the request was not stored",
        ),
        ConversationError::ReadOnly => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "read_only",
            "service is read-only",
        ),
        ConversationError::Internal(msg) => {
            api_error(StatusCode::INTERNAL_SERVER_ERROR, "internal", msg.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status_of(resp: Response) -> StatusCode {
        resp.status()
    }

    #[test]
    fn cross_tenant_is_indistinguishable_from_missing() {
        assert_eq!(
            status_of(map_context_error(&ContextError::CrossTenant)),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status_of(map_context_error(&ContextError::NotFound)),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn store_unavailable_is_retryable_not_a_silent_success() {
        assert_eq!(
            status_of(map_context_error(&ContextError::Unavailable)),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn chain_failures_are_client_errors_with_distinct_codes() {
        for err in [
            ContextError::NotStored,
            ContextError::ChainBroken("resp_x_1".into()),
            ContextError::ChainTooLong { limit: 50 },
            ContextError::ChainTooLarge { limit: 1024 },
        ] {
            assert_eq!(
                status_of(map_context_error(&err)),
                StatusCode::BAD_REQUEST,
                "{err:?} should be a client error"
            );
        }
    }

    #[test]
    fn integrity_mismatch_is_never_masked_as_success() {
        assert_eq!(
            status_of(map_context_error(&ContextError::IntegrityMismatch)),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn missing_sessions_and_conversations_are_also_plain_404s() {
        // SEC-2 applies to every resource: an id belonging to another tenant must
        // be indistinguishable from one that does not exist.
        assert_eq!(
            status_of(map_session_error(&SessionError::NotFound)),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status_of(map_conversation_error(&ConversationError::NotFound)),
            StatusCode::NOT_FOUND
        );
    }

    fn busy() -> SessionError {
        SessionError::Busy {
            holder: nova_responses_core::ResponseId::new(
                nova_responses_core::NodeTag::parse("n1").unwrap(),
            ),
        }
    }

    #[test]
    fn a_busy_session_is_a_conflict_not_a_queue() {
        // The caller is told, never silently queued behind the running turn.
        assert_eq!(status_of(map_session_error(&busy())), StatusCode::CONFLICT);
    }

    #[test]
    fn a_busy_message_names_the_holder() {
        // So the caller can subscribe to the turn it collided with rather than
        // poll to find out when the session frees up.
        let holder = nova_responses_core::ResponseId::new(
            nova_responses_core::NodeTag::parse("n1").unwrap(),
        );
        let (_, _, message) = map_session_stream_error(&SessionError::Busy {
            holder: holder.clone(),
        });
        assert!(message.contains(&holder.to_string()), "{message}");
    }

    #[test]
    fn taking_a_conversation_is_a_conflict_too() {
        // Not a 404 (the conversation exists) and not a 400 (the request was
        // well formed) — the caller was simply second.
        assert_eq!(
            status_of(map_session_error(&SessionError::ConversationTaken)),
            StatusCode::CONFLICT
        );
    }

    #[test]
    fn unavailable_stores_never_report_success() {
        for status in [
            status_of(map_session_error(&SessionError::Unavailable)),
            status_of(map_conversation_error(&ConversationError::Unavailable)),
        ] {
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        }
    }

    #[test]
    fn the_streaming_and_response_forms_of_a_session_error_agree() {
        // Two translations of one error would drift; the response form is built
        // from the streaming form so they cannot.
        for err in [
            SessionError::NotFound,
            busy(),
            SessionError::ConversationTaken,
            SessionError::CapacityExceeded,
            SessionError::Unavailable,
            SessionError::ReadOnly,
            SessionError::Internal("x".into()),
        ] {
            assert_eq!(
                map_session_stream_error(&err).0,
                status_of(map_session_error(&err)),
                "{err:?}"
            );
        }
    }
}
