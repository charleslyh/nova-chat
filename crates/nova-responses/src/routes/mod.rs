//! Route registration.
//!
//! Gone with D20: every `/v1/sessions/*` endpoint and `/v1/admin/trim_hot`.
//!
//! Gone with D23, and still gone under D25: `/v1/agent/claim`, `/heartbeat`,
//! `/append`, `/complete`. Execution is not a protocol — `nova-agentd` claims
//! from the shared ledger through the `ResponseLedger` port, so an HTTP pull
//! surface would only add a hop, a second authorisation path and a second place
//! for the attempt fence to be checked.

pub mod admin;
pub mod responses;

use axum::routing::{get, post};
use axum::Router;
use tower_http::cors::CorsLayer;

use crate::state::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(admin::health))
        // Protocol surface.
        .route("/v1/responses", post(responses::create))
        .route(
            "/v1/responses/{id}",
            get(responses::retrieve).delete(responses::delete),
        )
        .route("/v1/responses/{id}/cancel", post(responses::cancel))
        // Operations.
        .route("/v1/admin/read_only", post(admin::set_read_only))
        .route("/v1/admin/pending_limit", post(admin::set_pending_limit))
        .route("/v1/tenants/{tenant}/purge", post(admin::purge_tenant))
        .layer(CorsLayer::permissive())
        .with_state(state)
}
