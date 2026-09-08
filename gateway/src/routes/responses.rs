//! `/v1/responses` — creation, retrieval, streaming, cancellation, deletion.
//!
//! 这里是**接入层**：协议解析、租户鉴权、HTTP 翻译。业务编排
//! （resolve_chain / 幂等 / 三种投递的领域语义 / 终态提交）委托给 `service` 层
//! （D25 ⑤），本文件不直接操作端口。
//!
//! 存储是共享载体（`nova-responses-mem-server`），任意节点直读，无节点间转发。

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use nova_responses::protocol::{preflight_unsupported, CreateResponseRequest};
use nova_responses::{
    ContextError, ConversationId, IdempotencyKey, ResponseId, StoredResponse, TenantId,
};
use serde::Deserialize;
use serde_json::Value;

use crate::error::{
    api_error, bad_request, map_context_error, map_conversation_error, map_ledger_error, not_found,
};
use crate::routes::shared::tenant_or_reject;
use nova_responses::service::{ContextSource, CreateResult, ServiceError};
use crate::sse::{map_event_log_error, open_stream, resolve_cursor};
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct StreamQuery {
    #[serde(default)]
    pub stream: Option<bool>,
    #[serde(default)]
    pub starting_after: Option<u64>,
}

fn parse_id(raw: &str) -> Result<ResponseId, Response> {
    // A malformed id is reported as absent rather than as a parse error, so
    // probing the id format reveals nothing (SEC-2).
    ResponseId::parse(raw).map_err(|_| not_found())
}

/// Map a capability-layer error onto an HTTP response, preserving the distinct
/// status of each port error (INV-43).
fn map_service_error(err: &ServiceError) -> Response {
    match err {
        ServiceError::Context(e) => map_context_error(e),
        ServiceError::Ledger(e) => map_ledger_error(e),
        ServiceError::EventLog(e) => {
            let (status, code, message) = map_event_log_error(e);
            api_error(status, code, message)
        }
        // Notably includes `Busy` → 409: a second concurrent turn on one
        // conversation is refused, never queued behind the running one.
        ServiceError::Conversation(e) => map_conversation_error(e),
    }
}

/// Decide which context this generation inherits, from the two upstream fields.
///
/// Validation has already rejected the case where both are present, so this only
/// has to parse — but it still parses *both* rather than short-circuiting on the
/// first, so a malformed value is reported as malformed either way.
fn context_source(request: &CreateResponseRequest) -> Result<ContextSource, Response> {
    if let Some(reference) = &request.conversation {
        let id = ConversationId::parse(reference.id()).map_err(|_| {
            bad_request(
                "invalid_request",
                "conversation is not a valid conversation id",
            )
        })?;
        return Ok(ContextSource::Conversation(id));
    }
    match &request.previous_response_id {
        None => Ok(ContextSource::Fresh),
        Some(raw) => {
            let id = ResponseId::parse(raw).map_err(|_| {
                bad_request(
                    "chain_broken",
                    "previous_response_id is not a valid response id",
                )
            })?;
            Ok(ContextSource::Previous(id))
        }
    }
}

/// POST /v1/responses
pub async fn create(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(raw): Json<Value>,
) -> Response {
    let tenant = match tenant_or_reject(&state, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };

    // Draining: refuse new work but keep serving reads and subscriptions, so a
    // rolling deploy costs nothing in flight (FR-34).
    if !state.is_accepting() {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "draining",
            "node is shutting down; retry against the service",
        );
    }

    // Named remedies for the fields callers are most likely to try, before the
    // generic unknown-field rejection kicks in.
    if let Some((field, hint)) = preflight_unsupported(&raw) {
        return bad_request("unsupported_parameter", format!("`{field}` {hint}"));
    }

    let request: CreateResponseRequest = match serde_json::from_value(raw) {
        Ok(req) => req,
        // Structural rejection: unknown fields, unknown item types, inline
        // binary. This matches upstream behaviour and is intentional (INV-50).
        Err(e) => return bad_request("invalid_request", e.to_string()),
    };

    let input_items = match request.validate(&state.cfg.input_limits) {
        Ok(items) => items,
        Err(e) => return bad_request("invalid_request", e.to_string()),
    };

    // Only parsing happens here. Resolving what the source *means* — a
    // conversation's tail, and the turn lock that goes with it — is the
    // capability layer's job, so no facade can bypass it.
    let source = match context_source(&request) {
        Ok(source) => source,
        Err(resp) => return resp,
    };

    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(|v| IdempotencyKey(v.to_string()));

    let result = match state
        .service
        .create(&tenant, &request, input_items, source, idempotency_key)
        .await
    {
        Ok(r) => r,
        Err(e) => return map_service_error(&e),
    };

    match result {
        CreateResult::Accepted { record } => {
            // Execution is an independent process (D25): it claims from the shared
            // ledger and writes increments to the shared event buffer. The gateway
            // only enqueues the response and serves the delivery mode.

            let response_id = record.response_id.clone();

            // Three delivery modes over one internal event stream.
            if request.stream {
                return open_stream(state.event_log.clone(), response_id, None).await;
            }
            if request.background {
                return (
                    StatusCode::ACCEPTED,
                    Json(response_object(&record)),
                )
                    .into_response();
            }
            match state
                .service
                .wait_terminal(&tenant, &response_id, &record)
                .await
            {
                Ok(record) => Json(response_object(&record)).into_response(),
                Err(e) => map_service_error(&e),
            }
        }
        CreateResult::Duplicate { existing } => Json(response_object(&existing)).into_response(),
        CreateResult::ReadOnly => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "read_only",
            "service is read-only",
        ),
        CreateResult::Overloaded => api_error(
            StatusCode::TOO_MANY_REQUESTS,
            "overloaded",
            "too many in-flight responses; retry later",
        ),
    }
}

/// GET /v1/responses/{id}
pub async fn retrieve(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<StreamQuery>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant_or_reject(&state, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let response_id = match parse_id(&id) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    if q.stream.unwrap_or(false) {
        return stream_impl(state, response_id, q.starting_after, headers, tenant).await;
    }

    match state.service.retrieve(&tenant, &response_id).await {
        Ok(Some(record)) => Json(response_object(&record)).into_response(),
        Ok(None) => not_found(),
        Err(e) => map_service_error(&e),
    }
}

async fn stream_impl(
    state: AppState,
    response_id: ResponseId,
    starting_after: Option<u64>,
    headers: HeaderMap,
    tenant: TenantId,
) -> Response {
    let last_event_id = headers.get("last-event-id").and_then(|v| v.to_str().ok());
    let cursor = resolve_cursor(starting_after, last_event_id);

    // Ownership is verified before any events are exposed. When the response was
    // not stored there is no record to check against, so fall through: the id is
    // unguessable and its buffer is short-lived.
    match state.context.get(&tenant, &response_id).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            if let Ok(Some(record)) = state.ledger.get(&response_id).await {
                if &record.tenant_id != &tenant {
                    return not_found();
                }
            }
        }
        Err(ContextError::Unavailable) => {} // reads may proceed
        Err(e) => return map_context_error(&e),
    }

    open_stream(state.event_log.clone(), response_id, cursor).await
}

/// POST /v1/responses/{id}/cancel
pub async fn cancel(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant_or_reject(&state, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let response_id = match parse_id(&id) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    match state.service.cancel(&tenant, &response_id).await {
        Ok(Some(record)) => Json(response_object(&record)).into_response(),
        Ok(None) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "id": response_id.to_string(),
                "object": "response",
                "status": "cancelled",
            })),
        )
            .into_response(),
        Err(e) => map_service_error(&e),
    }
}

/// DELETE /v1/responses/{id}
pub async fn delete(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant_or_reject(&state, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let response_id = match parse_id(&id) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    match state.service.delete(&tenant, &response_id).await {
        Ok(true) => {
            Json(serde_json::json!({
                "id": response_id.to_string(),
                "object": "response.deleted",
                "deleted": true,
            }))
            .into_response()
        }
        Ok(false) => not_found(),
        Err(e) => map_service_error(&e),
    }
}

/// Response object in protocol shape.
///
/// `instructions` is echoed here — that is its only role. It is never part of
/// `input`/`output` items and never enters a chain (INV-49).
pub fn response_object(record: &StoredResponse) -> Value {
    record.to_response_value()
}
