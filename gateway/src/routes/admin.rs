//! Operational endpoints.
//!
//! `trim_hot` is gone with the cold tier. Tenant purge is new and gated behind
//! the admin credential.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use nova_responses::ports::metric;
use nova_responses::TenantId;

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
    let conversations = match state.conversation_repo.delete_by_tenant(&tenant).await {
        Ok(n) => n,
        Err(e) => return map_conversation_error(&e),
    };
    match state.ledger.delete_by_tenant(&tenant).await {
        Ok(deleted) => {
            state.metrics.incr(metric::TENANT_PURGES, 1);
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
    let conversation_ok = state.conversation_repo.health().await.is_ok();
    let ok = conversation_ok;

    let status = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(serde_json::json!({
            "ok": ok,
            "node_tag": state.responses_cfg().node_tag.as_str(),
            "accepting": state.is_accepting(),
            "stores": {
                "conversation": conversation_ok,
            },
        })),
    )
        .into_response()
}
