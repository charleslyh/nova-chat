//! Operational endpoints.
//!
//! `trim_hot` is gone with the cold tier. Tenant purge is new and gated behind
//! the admin credential.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use nova_responses::TenantId;
use serde::Deserialize;

use crate::error::{api_error, bad_request, map_conversation_error, map_ledger_error};
use crate::routes::shared::Reject;
use crate::state::AppState;

fn require_admin(state: &AppState, headers: &HeaderMap) -> Result<(), Reject> {
    state.keys.authorize_admin(headers).map_err(|_| {
        Box::new(api_error(
            StatusCode::UNAUTHORIZED,
            "invalid_credentials",
            "admin credentials required",
        ))
    })
}

#[derive(Debug, Deserialize)]
pub struct ReadOnlyBody {
    pub enabled: bool,
}

pub async fn set_read_only(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ReadOnlyBody>,
) -> Response {
    if let Err(resp) = require_admin(&state, &headers) {
        return *resp;
    }
    // Applies to this node only: nodes are peers, so there is no authority to
    // broadcast from.
    state.ledger.set_read_only(body.enabled);
    Json(serde_json::json!({
        "ok": true,
        "read_only": state.ledger.is_read_only(),
        "node_tag": state.cfg.node_tag.as_str(),
    }))
    .into_response()
}

#[derive(Debug, Deserialize)]
pub struct PendingLimitBody {
    pub pending_limit: usize,
}

pub async fn set_pending_limit(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<PendingLimitBody>,
) -> Response {
    if let Err(resp) = require_admin(&state, &headers) {
        return *resp;
    }
    state.ledger.set_pending_limit(body.pending_limit);
    Json(serde_json::json!({
        "ok": true,
        "pending_limit": state.ledger.pending_limit(),
    }))
    .into_response()
}

/// POST /v1/tenants/{tenant}/purge — bulk erasure (FR-21).
pub async fn purge_tenant(
    State(state): State<AppState>,
    Path(tenant_raw): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = require_admin(&state, &headers) {
        return *resp;
    }
    let Ok(tenant) = TenantId::parse(&tenant_raw) else {
        return bad_request("invalid_tenant", "tenant id is malformed");
    };

    // Erasure must cover every store holding tenant data, or "purged" would be a
    // false claim. Conversations first (their snapshots and event streams go with
    // them), then the tenant's response records (D30).
    let conversations = match state.conversation_store.delete_by_tenant(&tenant).await {
        Ok(n) => n,
        Err(e) => return map_conversation_error(&e),
    };
    match state.ledger.delete_by_tenant(&tenant).await {
        Ok(deleted) => {
            state.metrics.incr("tenant_purges", 1);
            Json(serde_json::json!({
                "ok": true,
                "tenant": tenant.as_str(),
                "deleted": deleted,
                "conversations_deleted": conversations,
            }))
            .into_response()
        }
        Err(e) => map_ledger_error(&e),
    }
}

/// GET /health
pub async fn health(State(state): State<AppState>) -> Response {
    // Store liveness is part of health: a node that cannot reach the durable
    // conversation store must not look healthy (INV-46).
    let conversation_ok = state.conversation_store.health().await.is_ok();
    let ok = conversation_ok;

    let in_flight = state.ledger.in_flight().await.unwrap_or(0);
    let status = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(serde_json::json!({
            "ok": ok,
            "node_tag": state.cfg.node_tag.as_str(),
            "accepting": state.is_accepting(),
            "read_only": state.ledger.is_read_only(),
            "in_flight": in_flight,
            "stores": {
                "conversation": conversation_ok,
            },
        })),
    )
        .into_response()
}
