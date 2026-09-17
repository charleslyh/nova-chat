//! `/v1/responses` — creation, retrieval, streaming, cancellation, deletion.
//!
//! 这里是**接入层**：协议解析、租户鉴权、HTTP 翻译。业务编排（上下文解析 / 幂等 / 三种
//! 投递的领域语义 / 终态提交）委托给能力层（D25 ⑤），本文件不直接操作端口。
//!
//! 存储是共享载体，任意节点直读，无节点间转发。

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use nova_responses::ports::CreateOutcome;
use nova_responses::protocol::{preflight_unsupported, CreateResponseRequest};
use nova_responses::service::ServiceError;
use nova_responses::{ContextAnchor, ConversationId, IdempotencyKey, ModelParams, ResponseId, TenantId, TurnSpec};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{
    api_error, bad_request, map_context_error, map_conversation_error, map_ledger_error, not_found,
};
use crate::routes::shared::{tenant_or_reject, Reject};
use crate::sse::{map_event_log_error, open_stream, resolve_cursor};
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct StreamQuery {
    #[serde(default)]
    pub stream: Option<bool>,
    #[serde(default)]
    pub starting_after: Option<u64>,
}

/// `DELETE` acknowledgement. A struct rather than a `json!` literal for the same reason
/// the response object is one: the shape is a contract, and a typo in a macro key is
/// invisible until a client breaks.
#[derive(Debug, Serialize)]
struct DeletedObject {
    id: String,
    object: &'static str,
    deleted: bool,
}

fn parse_id(raw: &str) -> Result<ResponseId, Reject> {
    // A malformed id is reported as absent rather than as a parse error, so probing the
    // id format reveals nothing (SEC-2).
    ResponseId::parse(raw).map_err(|_| Box::new(not_found()))
}

/// Map a capability-layer error onto an HTTP response, preserving the distinct status of
/// each failure class (INV-43).
fn map_service_error(err: &ServiceError) -> Response {
    match err {
        ServiceError::Ledger(e) => map_ledger_error(e),
        ServiceError::EventLog(e) => {
            let (status, code, message) = map_event_log_error(e);
            api_error(status, code, message)
        }
        // Notably includes `Busy` → 409: a second concurrent turn on one conversation is
        // refused, never queued behind the running one.
        ServiceError::Conversation(e) => map_conversation_error(e),
        ServiceError::Context(e) => map_context_error(e),
    }
}

/// Decide which context this generation inherits, from the two upstream fields.
///
/// Validation has already rejected the case where both are present, so this only has to
/// parse — but it still parses *both* rather than short-circuiting on the first, so a
/// malformed value is reported as malformed either way.
fn context_anchor(request: &CreateResponseRequest) -> Result<ContextAnchor, Reject> {
    if let Some(reference) = &request.conversation {
        let id = ConversationId::parse(reference.id()).map_err(|_| {
            Box::new(bad_request(
                "invalid_request",
                "conversation is not a valid conversation id",
            ))
        })?;
        return Ok(ContextAnchor::Conversation(id));
    }
    match &request.previous_response_id {
        None => Ok(ContextAnchor::Root),
        Some(raw) => {
            let id = ResponseId::parse(raw).map_err(|_| {
                Box::new(bad_request(
                    "chain_broken",
                    "previous_response_id is not a valid response id",
                ))
            })?;
            Ok(ContextAnchor::Previous(id))
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
        Err(resp) => return *resp,
    };

    // Draining: refuse new work but keep serving reads and subscriptions, so a rolling
    // deploy costs nothing in flight (FR-34).
    if !state.is_accepting() {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "draining",
            "node is shutting down; retry against the service",
        );
    }

    // Named remedies for the fields callers are most likely to try, before the generic
    // unknown-field rejection kicks in.
    if let Some((field, hint)) = preflight_unsupported(&raw) {
        return bad_request("unsupported_parameter", format!("`{field}` {hint}"));
    }

    let request: CreateResponseRequest = match serde_json::from_value(raw) {
        Ok(req) => req,
        // Structural rejection: unknown fields, unknown item types, inline binary. This
        // matches upstream behaviour and is intentional (INV-50).
        Err(e) => return bad_request("invalid_request", e.to_string()),
    };

    let input_items = match request.validate(&state.responses_cfg().limits) {
        Ok(items) => items,
        Err(e) => return bad_request("invalid_request", e.to_string()),
    };

    // Only parsing happens here. Resolving what the anchor *means* — a conversation's
    // tail, and the turn lock that goes with it — is the capability layer's job, so no
    // facade can bypass it.
    let anchor = match context_anchor(&request) {
        Ok(anchor) => anchor,
        Err(resp) => return *resp,
    };

    // The key becomes a storage lookup key, so it is validated at the boundary like every
    // other identity string (bounded length, visible ASCII).
    let idempotency_key = match headers.get("idempotency-key") {
        None => None,
        Some(value) => match value.to_str().ok().map(IdempotencyKey::parse) {
            Some(Ok(key)) => Some(key),
            _ => {
                return bad_request(
                    "invalid_request",
                    "idempotency-key header is empty, too long, or contains whitespace",
                )
            }
        },
    };

    // Resolve the wire request into the domain's own description of a turn: the `input`
    // shorthand and the `conversation` reference shape are gateway concerns, settled
    // here, so the capability layer sees only domain values.
    let spec = TurnSpec {
        params: ModelParams {
            model: request.model.clone(),
            instructions: request.instructions.clone(),
            tools: request.tools.clone().unwrap_or_default(),
            tool_choice: request.tool_choice.clone(),
            metadata: request.metadata.clone().unwrap_or_default(),
        },
        input_items,
        store: request.store,
        ext: request.ext.clone(),
        anchor,
    };

    let outcome = match state.service.create(&tenant, spec, idempotency_key).await {
        Ok(outcome) => outcome,
        Err(e) => return map_service_error(&e),
    };

    match outcome {
        CreateOutcome::Accepted(record) => {
            // Execution is an independent process (D25): it claims from the shared ledger
            // and writes increments to the shared event buffer. The gateway only enqueues
            // the response and serves the delivery mode.
            if request.stream {
                let response_id = record.response_id.clone();
                return open_stream(state.event_log.clone(), response_id, None).await;
            }
            if request.background {
                return (
                    StatusCode::ACCEPTED,
                    Json(nova_responses::protocol::ResponseObject::without_output(
                        &record,
                    )),
                )
                    .into_response();
            }
            match state.service.wait_terminal(&record).await {
                Ok(object) => Json(object).into_response(),
                Err(e) => map_service_error(&e),
            }
        }
        // Idempotent replay returns the original generation, never a second one (FR-3).
        CreateOutcome::Duplicate(existing) => Json(
            nova_responses::protocol::ResponseObject::without_output(&existing),
        )
        .into_response(),
        CreateOutcome::ReadOnly => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "read_only",
            "service is read-only",
        ),
        CreateOutcome::Overloaded => api_error(
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
        Err(resp) => return *resp,
    };
    let response_id = match parse_id(&id) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };

    if q.stream.unwrap_or(false) {
        return stream_impl(state, response_id, q.starting_after, headers, tenant).await;
    }

    match state.service.retrieve(&tenant, &response_id).await {
        Ok(Some(object)) => Json(object).into_response(),
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

    // Ownership is verified before any events are exposed, via the ledger record (D30).
    // The response itself is reconstructable from the event stream; a missing ledger
    // record means the response never existed or was deleted.
    if let Ok(Some(record)) = state.ledger.get(&response_id).await {
        if record.tenant_id != tenant {
            return not_found();
        }
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
        Err(resp) => return *resp,
    };
    let response_id = match parse_id(&id) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };

    // Cancel either fails or produces an object — there is no third outcome for this
    // layer to invent a body for.
    match state.service.cancel(&tenant, &response_id).await {
        Ok(object) => Json(object).into_response(),
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
        Err(resp) => return *resp,
    };
    let response_id = match parse_id(&id) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };

    match state.service.delete(&tenant, &response_id).await {
        Ok(true) => Json(DeletedObject {
            id: response_id.to_string(),
            object: "response.deleted",
            deleted: true,
        })
        .into_response(),
        Ok(false) => not_found(),
        Err(e) => map_service_error(&e),
    }
}
