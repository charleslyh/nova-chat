//! Execution-side endpoints: claim, heartbeat, append, complete.
//!
//! The important one is [`complete`]: it accepts the **normalised final output
//! items** from the execution side and writes them to the context store
//! directly. Nothing here reconstructs output by replaying the event stream
//! (INV-48) — doing so would force the event log to become a durable source of
//! truth and collapse the whole storage boundary.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use nova_responses_core::{
    AgentId, Attempt, ResponseEvent, ResponseEventKind, ResponseId, ResponseItem, ResponseStatus,
    Usage,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{api_error, bad_request, map_context_error, map_ledger_error, not_found};
use crate::sse::map_event_log_error;
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct ClaimBody {
    pub agent_id: Uuid,
}

#[derive(Serialize)]
pub struct ClaimResponse {
    pub response_id: String,
    pub attempt: Attempt,
    pub model: String,
    /// Full context to send to the model: resolved history plus this turn's
    /// input. Assembled server-side, which is the entire point of the chain.
    pub input: Vec<ResponseItem>,
    /// Prepended as a system/developer message by the execution side. Delivered
    /// separately from `input` because it is not an item.
    pub instructions: Option<String>,
    pub exec_deadline_ms: u64,
}

pub async fn claim(State(state): State<AppState>, Json(body): Json<ClaimBody>) -> Response {
    let agent = AgentId(body.agent_id);
    let now = state.now_ms().await;
    match state.ledger.claim(agent, now, state.cfg.exec_ttl_ms).await {
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Ok(Some(claimed)) => {
            let record = &claimed.record;

            // Rebuild the full prompt context: history from the chain, then this
            // turn's own input.
            let mut input = Vec::new();
            if let Some(previous) = &record.previous_response_id {
                match state
                    .context
                    .resolve_chain(&record.tenant_id, previous, state.cfg.chain_limits)
                    .await
                {
                    Ok(resolved) => input.extend(resolved.items),
                    // The chain broke between creation and claiming. Report it
                    // rather than silently running a single-turn prompt, which
                    // would look like the model forgetting context.
                    Err(e) => return map_context_error(&e),
                }
            }
            input.extend(record.input_items.iter().cloned());

            let _ = state
                .event_log
                .append(ResponseEvent {
                    response_id: record.response_id.clone(),
                    sequence_number: 0,
                    kind: ResponseEventKind::InProgress,
                    attempt: Some(claimed.attempt),
                    payload: String::new(),
                })
                .await;

            Json(ClaimResponse {
                response_id: record.response_id.to_string(),
                attempt: claimed.attempt,
                model: record.model.clone(),
                input,
                instructions: record.instructions.clone(),
                exec_deadline_ms: claimed.exec_deadline_ms,
            })
            .into_response()
        }
        Err(e) => map_ledger_error(&e),
    }
}

#[derive(Debug, Deserialize)]
pub struct HeartbeatBody {
    pub agent_id: Uuid,
}

pub async fn heartbeat(State(state): State<AppState>, Json(body): Json<HeartbeatBody>) -> Response {
    let now = state.now_ms().await;
    let _ = state.ledger.heartbeat(AgentId(body.agent_id), now).await;
    StatusCode::NO_CONTENT.into_response()
}

#[derive(Debug, Deserialize)]
pub struct AppendBody {
    pub response_id: String,
    pub attempt: u64,
    /// Protocol event name, e.g. `response.output_text.delta`.
    pub kind: String,
    #[serde(default)]
    pub payload: String,
}

pub async fn append(State(state): State<AppState>, Json(body): Json<AppendBody>) -> Response {
    let Ok(response_id) = ResponseId::parse(&body.response_id) else {
        return not_found();
    };
    let kind: ResponseEventKind = match serde_json::from_value(Value::String(body.kind.clone())) {
        Ok(k) => k,
        Err(_) => {
            return bad_request(
                "unknown_event_type",
                format!("`{}` is not a protocol event name", body.kind),
            )
        }
    };

    match state
        .event_log
        .append(ResponseEvent {
            response_id,
            sequence_number: 0,
            kind,
            attempt: Some(Attempt(body.attempt)),
            payload: body.payload,
        })
        .await
    {
        Ok(sequence_number) => Json(serde_json::json!({ "sequence_number": sequence_number }))
            .into_response(),
        Err(e) => {
            let (status, code, message) = map_event_log_error(&e);
            api_error(status, code, message)
        }
    }
}

use serde_json::Value;

#[derive(Debug, Deserialize)]
pub struct CompleteBody {
    pub response_id: String,
    pub attempt: u64,
    #[serde(default = "default_true")]
    pub ok: bool,
    /// Normalised final output items, supplied directly by the execution side.
    #[serde(default)]
    pub output: Vec<ResponseItem>,
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
}

fn default_true() -> bool {
    true
}

pub async fn complete(State(state): State<AppState>, Json(raw): Json<Value>) -> Response {
    // Parsed by hand rather than through `Json<CompleteBody>`: axum's extractor
    // rejects a malformed body with 422, but the protocol contract says 400 for
    // every rejected payload. Taking `Json<Value>` first keeps that promise —
    // and this endpoint is exactly where an unacceptable output item type must
    // be reported precisely (chain closure, below).
    let body: CompleteBody = match serde_json::from_value(raw) {
        Ok(b) => b,
        Err(e) => return bad_request("invalid_request", e.to_string()),
    };

    let Ok(response_id) = ResponseId::parse(&body.response_id) else {
        return not_found();
    };
    let attempt = Attempt(body.attempt);
    let now = state.now_ms().await;

    // Chain closure (INV-47): whatever we emit must be acceptable as input on
    // the next turn, otherwise our own chain breaks. Checked here, at the only
    // place output enters the system.
    for (index, item) in body.output.iter().enumerate() {
        if !item.is_acceptable_as_input() {
            return bad_request(
                "chain_closure_violation",
                format!(
                    "output item {index} of type `{}` would not be accepted as input",
                    item.item_type()
                ),
            );
        }
        if let Err(e) = item.validate() {
            return bad_request("invalid_output_item", format!("item {index}: {e}"));
        }
    }

    let status = if body.ok {
        ResponseStatus::Completed
    } else {
        ResponseStatus::Failed
    };
    let usage = Usage::new(body.input_tokens, body.output_tokens);

    let record = match state.ledger.get(&response_id).await {
        Ok(Some(r)) => r,
        Ok(None) => return not_found(),
        Err(e) => return map_ledger_error(&e),
    };

    if let Err(e) = state
        .ledger
        .complete(&response_id, attempt, status, usage, now)
        .await
    {
        return map_ledger_error(&e);
    }

    // Persist the submitted output. Only meaningful when storing was requested.
    if record.stored {
        if let Err(e) = state
            .context
            .append_output(
                &record.tenant_id,
                &response_id,
                body.output.clone(),
                usage,
                status,
                now,
            )
            .await
        {
            return map_context_error(&e);
        }
    }

    let terminal_kind = if body.ok {
        ResponseEventKind::Completed
    } else {
        ResponseEventKind::Failed
    };
    // Deliberately no `attempt` on envelope events the *server* emits.
    //
    // The fence (INV-6) exists to stop a superseded *execution side* from
    // writing. By this point the ledger has already verified `expected_attempt`
    // and moved the response to a terminal state — so re-checking here would
    // reject the very event that announces the transition, and the stream would
    // never terminate. Reaping and cancellation emit their envelopes the same
    // way, for the same reason.
    let _ = state
        .event_log
        .append(ResponseEvent {
            response_id: response_id.clone(),
            sequence_number: 0,
            kind: terminal_kind,
            attempt: None,
            payload: String::new(),
        })
        .await;
    // Starts the retention window; the buffer is released after it elapses.
    let _ = state
        .event_log
        .close(&response_id, now, state.cfg.retain_after_terminal_ms)
        .await;

    state.metrics.incr("responses_completed", 1).await;
    StatusCode::NO_CONTENT.into_response()
}
