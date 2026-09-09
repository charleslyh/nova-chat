//! Ingress helpers shared by the protocol route modules.
//!
//! These exist once because they encode security decisions, and a second copy is
//! a second chance to get one of them wrong: credential rejection must not
//! distinguish "no key" from "bad key" any more than it has to, and a malformed
//! id must read as absent rather than as a parse error (SEC-2).

use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use nova_responses::TenantId;

use crate::auth::AuthError;
use crate::error::{api_error, not_found_kind};
use crate::state::AppState;

/// The rejection half of an ingress check, boxed so `Result<T, Reject>` stays
/// small on the happy path (`clippy::result_large_err`).
pub type Reject = Box<Response>;

pub fn tenant_or_reject(state: &AppState, headers: &HeaderMap) -> Result<TenantId, Reject> {
    state.keys.resolve(headers).map_err(|e| {
        Box::new(match e {
            AuthError::Missing => api_error(
                StatusCode::UNAUTHORIZED,
                "missing_credentials",
                "provide `Authorization: Bearer <key>`",
            ),
            AuthError::Invalid => api_error(
                StatusCode::UNAUTHORIZED,
                "invalid_credentials",
                "credentials were rejected",
            ),
        })
    })
}

/// Turn an id parse failure into a 404 for `kind`.
///
/// Reporting the parse failure instead would let a caller probe the id format,
/// and — worse — distinguish "this shape is wrong" from "this id is not yours",
/// which is exactly the distinction SEC-2 removes.
pub fn parse_or_not_found<T, E>(parsed: Result<T, E>, kind: &str) -> Result<T, Reject> {
    parsed.map_err(|_| Box::new(not_found_kind(kind)))
}
