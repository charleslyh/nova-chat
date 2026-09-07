//! Route registration.
//!
//! Two surfaces, deliberately kept in separate namespaces:
//!
//! - `/v1/responses` and `/v1/conversations` are **upstream's**, matched
//!   endpoint for endpoint so an official SDK can drive them unmodified. Note
//!   that `/v1/conversations/{id}` handles both `GET` and `POST`, because
//!   upstream models the metadata update as a POST to the same path rather than
//!   a PATCH.
//! - `/v1/conversations` also carries our **self-hosted** sub-resources (D28):
//!   the event stream (`/events`), business events ordered alongside the
//!   conversation, and the whole history in one call (`/transcript`) — three
//!   things upstream has no protocol for, folded onto the one container rather
//!   than a parallel surface.
//!
//! There is no self-hosted endpoint for *starting* a turn. That happens through
//! the standard `POST /v1/responses` carrying `conversation`; the service finds
//! the owning conversation and takes its turn lock.
//!
//! Gone with D23, and still gone under D25: `/v1/agent/claim`, `/heartbeat`,
//! `/append`, `/complete`. Execution is not a protocol — `nova-agentd` claims
//! from the shared ledger through the `ResponseLedger` port, so an HTTP pull
//! surface would only add a hop, a second authorisation path and a second place
//! for the attempt fence to be checked.

pub mod admin;
pub mod conversations;
pub mod responses;
pub(crate) mod shared;

use axum::routing::{get, post};
use axum::Router;
use tower_http::cors::CorsLayer;

use crate::state::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(admin::health))
        // Upstream-compatible surface.
        .route("/v1/responses", post(responses::create))
        .route(
            "/v1/responses/{id}",
            get(responses::retrieve).delete(responses::delete),
        )
        .route("/v1/responses/{id}/cancel", post(responses::cancel))
        // `/v1/conversations` is the single resource (D28): the official CRUD
        // above plus the self-hosted sub-resources (list / events / transcript)
        // that upstream has no protocol for.
        .route("/v1/conversations", get(conversations::list).post(conversations::create))
        .route(
            "/v1/conversations/{id}",
            get(conversations::retrieve)
                .post(conversations::update)
                .delete(conversations::delete),
        )
        .route(
            "/v1/conversations/{id}/events",
            get(conversations::events).post(conversations::append_event),
        )
        .route(
            "/v1/conversations/{id}/transcript",
            get(conversations::transcript),
        )
        // Operations.
        .route("/v1/admin/read_only", post(admin::set_read_only))
        .route("/v1/admin/pending_limit", post(admin::set_pending_limit))
        .route("/v1/tenants/{tenant}/purge", post(admin::purge_tenant))
        .layer(CorsLayer::permissive())
        .with_state(state)
}
