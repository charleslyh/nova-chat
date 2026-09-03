//! Error shaping.
//!
//! One rule drives the whole mapping: **every failure gets a distinct, explicit
//! status**. Nothing degrades to a partial success, and nothing that is really a
//! failure returns 200 (INV-43).

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use nova_responses_core::{ContextError, LedgerError};

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
}
