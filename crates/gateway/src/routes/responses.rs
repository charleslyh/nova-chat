//! `/v1/responses` — creation, retrieval, streaming, cancellation, deletion.

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use nova_responses_core::protocol::{preflight_unsupported, CreateResponseRequest};
use nova_responses_core::{
    Attempt, ContextError, CreateOutcome, IdempotencyKey, ResponseEvent, ResponseEventKind,
    ResponseId, ResponseItem, ResponseStatus, StoredResponse, TenantId, Usage,
};
use serde::Deserialize;
use serde_json::Value;

use crate::auth::AuthError;
use crate::error::{api_error, bad_request, map_context_error, map_ledger_error, not_found};
use crate::routing::{
    proxy_delete, proxy_get, proxy_post, route_chain_affinity, route_content, route_inflight, Route,
};
use crate::sse::{map_event_log_error, open_stream, resolve_cursor};
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct StreamQuery {
    #[serde(default)]
    pub stream: Option<bool>,
    #[serde(default)]
    pub starting_after: Option<u64>,
}

fn tenant_or_reject(state: &AppState, headers: &HeaderMap) -> Result<TenantId, Response> {
    state.keys.resolve(headers).map_err(|e| match e {
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
}

fn parse_id(raw: &str) -> Result<ResponseId, Response> {
    // A malformed id is reported as absent rather than as a parse error, so
    // probing the id format reveals nothing (SEC-2).
    ResponseId::parse(raw).map_err(|_| not_found())
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

    // Chain affinity: while content is not shared, hand the create to the node
    // owning the previous link so the walk stays local. Disappears by itself
    // once `is_shared()` is true.
    if let Some(previous_raw) = &request.previous_response_id {
        let previous = match parse_id(previous_raw) {
            Ok(id) => id,
            Err(_) => {
                return bad_request(
                    "chain_broken",
                    "previous_response_id is not a valid response id",
                )
            }
        };
        match route_chain_affinity(&state, &previous) {
            Route::Local => {}
            Route::UnknownNode => {
                return bad_request(
                    "chain_broken",
                    "previous_response_id refers to an unknown node",
                )
            }
            Route::Peer(peer) => {
                let body = serde_json::to_value(&request).unwrap_or(Value::Null);
                return match proxy_post(&state, &peer, "/v1/responses", &tenant, &body).await {
                    Ok(resp) => resp,
                    Err((status, msg)) => api_error(status, "upstream_error", msg),
                };
            }
        }
    }

    let now = state.now_ms().await;

    // Resolve history *before* creating anything: a broken chain must not leave
    // a half-created response behind.
    //
    // The resolved history is snapshotted onto the new record (D24): from here on
    // this response carries its full context, so a later deletion of one of its
    // ancestors cannot strand it.
    let mut snapshot: Vec<ResponseItem> = Vec::new();
    let mut snapshot_depth: usize = 0;
    if let Some(previous_raw) = &request.previous_response_id {
        let previous = match parse_id(previous_raw) {
            Ok(id) => id,
            Err(_) => {
                return bad_request(
                    "chain_broken",
                    "previous_response_id is not a valid response id",
                )
            }
        };
        match state
            .context
            .resolve_chain(&tenant, &previous, state.cfg.chain_limits)
            .await
        {
            Ok(resolved) => {
                state
                    .metrics
                    .incr("chain_resolved_depth", resolved.depth as u64)
                    .await;
                // Materialise the whole history as a flat copy, plus how many
                // turns it spans (D24).
                snapshot = resolved.items;
                snapshot_depth = resolved.depth;
            }
            Err(e) => return map_context_error(&e),
        }
    }

    // Storing is refused rather than skipped when the store is down, otherwise
    // the chain would break silently on a later turn (INV-46).
    if request.store {
        if let Err(e) = state.context.health().await {
            return map_context_error(&e);
        }
    }

    let response_id = ResponseId::new(state.cfg.node_tag.clone());
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(|v| IdempotencyKey(v.to_string()))
        .unwrap_or_else(|| IdempotencyKey(response_id.to_string()));

    let expires_at_ms = if request.store {
        Some(now.saturating_add(state.cfg.content_retention_ms))
    } else {
        None
    };

    let record = StoredResponse {
        response_id: response_id.clone(),
        previous_response_id: request
            .previous_response_id
            .as_deref()
            .and_then(|v| ResponseId::parse(v).ok()),
        tenant_id: tenant.clone(),
        model: request.model.clone(),
        // Stored for echo on retrieval; never fed into a later chain (INV-49).
        instructions: request.instructions.clone(),
        input_items: input_items.clone(),
        output_items: Vec::new(),
        status: ResponseStatus::Queued,
        usage: Usage::default(),
        created_at_ms: now,
        completed_at_ms: None,
        stored: request.store,
        expires_at_ms,
        integrity: None,
        integrity_alg: None,
        node_tag: state.cfg.node_tag.clone(),
        idempotency_key: Some(idempotency_key.clone()),
        owner: None,
        attempt: Attempt::default(),
        context: snapshot,
        context_depth: snapshot_depth,
    };

    match state
        .ledger
        .create(record.clone(), idempotency_key, now)
        .await
    {
        Ok(CreateOutcome::Accepted { .. }) => {}
        Ok(CreateOutcome::Duplicate { response_id }) => {
            // Idempotent replay returns the original, never a second response.
            return match state.context.get(&tenant, &response_id).await {
                Ok(Some(existing)) => Json(response_object(&existing, &[])).into_response(),
                Ok(None) => not_found(),
                Err(e) => map_context_error(&e),
            };
        }
        Ok(CreateOutcome::ReadOnly) => {
            return api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "read_only",
                "service is read-only",
            )
        }
        Ok(CreateOutcome::Overloaded) => {
            return api_error(
                StatusCode::TOO_MANY_REQUESTS,
                "overloaded",
                "too many in-flight responses; retry later",
            )
        }
        Err(e) => return map_ledger_error(&e),
    }

    if request.store {
        if let Err(e) = state.context.put(record.clone()).await {
            return map_context_error(&e);
        }
    }

    // First event, so a subscriber attaching immediately sees a defined start.
    let created = ResponseEvent {
        response_id: response_id.clone(),
        sequence_number: 0,
        kind: ResponseEventKind::Created,
        attempt: None,
        payload: String::new(),
    };
    if let Err(e) = state.event_log.append(created).await {
        let (status, code, message) = map_event_log_error(&e);
        return api_error(status, code, message);
    }

    state.metrics.incr("responses_created", 1).await;

    // Hand it to this node's execution engine. In process, and on this node
    // specifically: this is the node holding the in-flight event buffer, so it is
    // the only one whose increments subscribers will be routed to (FR-4 / D23).
    state.notify_work();

    // Three delivery modes over one internal event stream.
    if request.stream {
        return open_stream(state.event_log.clone(), response_id, None).await;
    }
    if request.background {
        return (
            StatusCode::ACCEPTED,
            Json(response_object(&record, &[])),
        )
            .into_response();
    }
    wait_for_terminal(&state, &tenant, &response_id, &record).await
}

/// Synchronous mode: wait for a terminal event, then assemble the full object.
///
/// On timeout the current state object is returned rather than an error, so the
/// caller can switch to polling — a timeout here is not a failure of the
/// generation.
async fn wait_for_terminal(
    state: &AppState,
    tenant: &TenantId,
    response_id: &ResponseId,
    fallback: &StoredResponse,
) -> Response {
    let budget_ms = state.cfg.sync_wait_timeout_ms;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(budget_ms);
    let mut cursor: Option<u64> = None;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match state
            .event_log
            .read_after(response_id, cursor, 256, remaining.as_millis() as u64)
            .await
        {
            Ok(batch) if !batch.is_empty() => {
                let terminal = batch.iter().any(|e| e.kind.is_terminal());
                cursor = batch.last().map(|e| e.sequence_number).or(cursor);
                if terminal {
                    break;
                }
            }
            Ok(_) => break,
            Err(e) => {
                let (status, code, message) = map_event_log_error(&e);
                return api_error(status, code, message);
            }
        }
    }

    match state.context.get(tenant, response_id).await {
        Ok(Some(record)) => Json(response_object(&record, &[])).into_response(),
        // store=false leaves nothing to read back, so answer from the in-memory
        // record we already hold.
        Ok(None) => Json(response_object(fallback, &[])).into_response(),
        Err(e) => map_context_error(&e),
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

    // Content read: direct when storage is shared, otherwise a hop to the owner.
    match route_content(&state, &response_id) {
        Route::Local => {}
        Route::UnknownNode => return not_found(),
        Route::Peer(peer) => {
            let path = format!("/v1/responses/{response_id}");
            return match proxy_get(&state, &peer, &path, &tenant, &headers).await {
                Ok(resp) => resp,
                Err((status, msg)) => api_error(status, "upstream_error", msg),
            };
        }
    }

    match state.context.get(&tenant, &response_id).await {
        Ok(Some(record)) => Json(response_object(&record, &[])).into_response(),
        Ok(None) => not_found(),
        Err(e) => map_context_error(&e),
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

    // In-flight events live in one process's heap: this hop is permanent and
    // cannot be replaced by a direct connection.
    match route_inflight(&state, &response_id) {
        Route::Local => {}
        Route::UnknownNode => return not_found(),
        Route::Peer(peer) => {
            let path = match cursor {
                Some(after) => format!("/v1/responses/{response_id}?stream=true&starting_after={after}"),
                None => format!("/v1/responses/{response_id}?stream=true"),
            };
            return match proxy_get(&state, &peer, &path, &tenant, &headers).await {
                Ok(resp) => resp,
                Err((status, msg)) => api_error(status, "upstream_error", msg),
            };
        }
    }

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

    match route_inflight(&state, &response_id) {
        Route::Local => {}
        Route::UnknownNode => return not_found(),
        Route::Peer(peer) => {
            let path = format!("/v1/responses/{response_id}/cancel");
            return match proxy_post(&state, &peer, &path, &tenant, &Value::Null).await {
                Ok(resp) => resp,
                Err((status, msg)) => api_error(status, "upstream_error", msg),
            };
        }
    }

    let now = state.now_ms().await;
    if let Err(e) = state.ledger.cancel(&tenant, &response_id, now).await {
        return map_ledger_error(&e);
    }

    // Terminal event, then close the buffer so its retention window starts.
    let _ = state
        .event_log
        .append(ResponseEvent {
            response_id: response_id.clone(),
            sequence_number: 0,
            kind: ResponseEventKind::Failed,
            attempt: None,
            payload: "cancelled".into(),
        })
        .await;
    let _ = state
        .event_log
        .close(&response_id, now, state.cfg.retain_after_terminal_ms)
        .await;
    state.metrics.incr("responses_cancelled", 1).await;

    match state.context.get(&tenant, &response_id).await {
        Ok(Some(record)) => Json(response_object(&record, &[])).into_response(),
        Ok(None) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "id": response_id.to_string(),
                "object": "response",
                "status": "cancelled",
            })),
        )
            .into_response(),
        Err(e) => map_context_error(&e),
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

    match route_content(&state, &response_id) {
        Route::Local => {}
        Route::UnknownNode => return not_found(),
        Route::Peer(peer) => {
            let path = format!("/v1/responses/{response_id}");
            return match proxy_delete(&state, &peer, &path, &tenant).await {
                Ok(resp) => resp,
                Err((status, msg)) => api_error(status, "upstream_error", msg),
            };
        }
    }

    match state.context.delete(&tenant, &response_id).await {
        Ok(true) => {
            state.metrics.incr("responses_deleted", 1).await;
            Json(serde_json::json!({
                "id": response_id.to_string(),
                "object": "response.deleted",
                "deleted": true,
            }))
            .into_response()
        }
        Ok(false) => not_found(),
        Err(e) => map_context_error(&e),
    }
}

/// Response object in protocol shape.
///
/// `instructions` is echoed here — that is its only role. It is never part of
/// `input`/`output` items and never enters a chain (INV-49).
pub fn response_object(record: &StoredResponse, _resolved: &[ResponseItem]) -> Value {
    serde_json::json!({
        "id": record.response_id.to_string(),
        "object": "response",
        "created_at": record.created_at_ms / 1000,
        "status": record.status.as_str(),
        "model": record.model,
        "previous_response_id": record.previous_response_id.as_ref().map(|v| v.to_string()),
        "instructions": record.instructions,
        "store": record.stored,
        "input": record.input_items,
        "output": record.output_items,
        "usage": {
            "input_tokens": record.usage.input_tokens,
            "output_tokens": record.usage.output_tokens,
            "total_tokens": record.usage.total_tokens,
        },
    })
}
