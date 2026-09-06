//! `/v1/sessions` — the self-hosted session layer (D26).
//!
//! 这里是**接入层**：协议解析、租户鉴权、游标解析、HTTP 翻译。业务编排委托给
//! `service` 层，本文件不直接操作端口。
//!
//! **没有自研的写生成端点**。轮次一律通过标准 `POST /v1/responses`（携带
//! `conversation`）发起，服务端据容器找到所属会话并取锁。会话层只提供官方协议里
//! 缺位的三件事：事件订阅、业务事件、一次性取回历史。

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use nova_responses_core::protocol::{AppendBusinessEventRequest, CreateSessionRequest};
use nova_responses_core::{
    ConversationId, LockState, ResolvedContext, Session, SessionId,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::{
    api_error, bad_request, map_conversation_error, map_session_error, not_found_kind,
};
use crate::routes::shared::{parse_or_not_found, tenant_or_reject};
use crate::service::conversations::TranscriptError;
use crate::service::SessionsServiceError;
use crate::sse::{open_session_stream, resolve_cursor};
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    #[serde(default)]
    pub starting_after: Option<u64>,
}

/// 领域对象 → 协议形状。
///
/// 展示常量（`object`、锁状态的字面量）只在这一层产生。
fn session_object(session: &Session) -> Value {
    json!({
        "id": session.id.to_string(),
        "object": "session",
        "created_at": session.created_at_ms / 1000,
        "conversation": { "id": session.conversation_id.to_string() },
        "status": match session.lock_state {
            LockState::Idle => "idle",
            LockState::Busy { .. } => "busy",
        },
        // 各端据此禁用或恢复输入。给出持有者而不只是布尔值，是为了让「正在生成的
        // 是哪一条」可被直接订阅逐字流，无需再查一次。
        "active_response_id": session.lock_state.holder().map(|id| id.to_string()),
    })
}

fn parse_id(raw: &str) -> Result<SessionId, Response> {
    parse_or_not_found(SessionId::parse(raw), "session")
}

fn map_sessions_error(err: &SessionsServiceError) -> Response {
    match err {
        SessionsServiceError::Session(e) => map_session_error(e),
        SessionsServiceError::Conversation(e) => map_conversation_error(e),
        SessionsServiceError::Transcript(e) => map_transcript_error(e),
    }
}

fn map_transcript_error(err: &TranscriptError) -> Response {
    match err {
        TranscriptError::Conversation(e) => map_conversation_error(e),
        TranscriptError::Context(e) => crate::error::map_context_error(e),
    }
}

fn draining() -> Response {
    api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "draining",
        "node is shutting down; retry against the service",
    )
}

/// POST /v1/sessions
pub async fn create(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(raw): Json<Value>,
) -> Response {
    let tenant = match tenant_or_reject(&state, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    if !state.is_accepting() {
        return draining();
    }

    let request: CreateSessionRequest = match serde_json::from_value(raw) {
        Ok(req) => req,
        Err(e) => return bad_request("invalid_request", e.to_string()),
    };

    let conversation = match &request.conversation {
        None => None,
        Some(reference) => match ConversationId::parse(reference.id()) {
            Ok(id) => Some(id),
            // 这里报 400 而不是 404：容器标识是请求体里的一个值，格式不合法属于请求
            // 本身有问题，与「路径寻址一个不存在的资源」不是一回事。
            Err(_) => {
                return bad_request(
                    "invalid_request",
                    "conversation is not a valid conversation id",
                )
            }
        },
    };

    match state.sessions.create(&tenant, conversation).await {
        Ok(session) => (StatusCode::OK, Json(session_object(&session))).into_response(),
        Err(e) => map_sessions_error(&e),
    }
}

/// GET /v1/sessions/{id}
pub async fn retrieve(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant_or_reject(&state, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let id = match parse_id(&id) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    match state.sessions.retrieve(&tenant, &id).await {
        Ok(Some(session)) => Json(session_object(&session)).into_response(),
        Ok(None) => not_found_kind("session"),
        Err(e) => map_session_error(&e),
    }
}

/// DELETE /v1/sessions/{id}
///
/// 不删除关联容器：容器承载对话历史，会话只承载广播状态。
pub async fn delete(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant_or_reject(&state, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let id = match parse_id(&id) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    match state.sessions.delete(&tenant, &id).await {
        Ok(true) => Json(json!({
            "id": id.to_string(),
            "object": "session.deleted",
            "deleted": true,
        }))
        .into_response(),
        Ok(false) => not_found_kind("session"),
        Err(e) => map_session_error(&e),
    }
}

/// GET /v1/sessions/{id}/events
///
/// 从游标开始：先回放持久历史，再转实时推送，一次调用完成。所以没有额外的快照接
/// 口，也就不存在会过期的快照。
pub async fn events(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<EventsQuery>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant_or_reject(&state, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let id = match parse_id(&id) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    // `Last-Event-ID` 优先于查询参数：它反映客户端**实际收到**了什么，而查询参数只
    // 是首次建连时被告知的值。断线重连因此不需要客户端自己记账。
    let last_event_id = headers.get("last-event-id").and_then(|v| v.to_str().ok());
    let cursor = resolve_cursor(q.starting_after, last_event_id);

    open_session_stream(
        state.session_store.clone(),
        tenant,
        id,
        cursor,
        state.cfg.session_events_page,
    )
    .await
}

/// POST /v1/sessions/{id}/events
///
/// 业务自定义事件，与对话事件同处一个序号空间、严格保序，且**不进入模型上下文**。
pub async fn append_event(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(raw): Json<Value>,
) -> Response {
    let tenant = match tenant_or_reject(&state, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    if !state.is_accepting() {
        return draining();
    }
    let id = match parse_id(&id) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    let request: AppendBusinessEventRequest = match serde_json::from_value(raw) {
        Ok(req) => req,
        // 信封字段封闭：未知字段一律 400。payload 内部不校验形状，但有体积与深度
        // 上界（见 protocol::session）。
        Err(e) => return bad_request("invalid_request", e.to_string()),
    };
    if let Err(e) = request.validate(&state.cfg.input_limits) {
        return bad_request("invalid_request", e.to_string());
    }

    match state
        .sessions
        .append_business_event(&tenant, &id, request.kind, request.payload)
        .await
    {
        Ok(seq) => (
            StatusCode::OK,
            Json(json!({
                "object": "session.event",
                "session_id": id.to_string(),
                // 返回序号，调用方可据此确认自己的事件排在了哪里。
                "seq": seq,
            })),
        )
            .into_response(),
        Err(e) => map_session_error(&e),
    }
}

/// GET /v1/sessions/{id}/transcript
///
/// 一次取回完整对话历史，不分页——链尾响应已携带全部祖先条目的扁平副本（D24），
/// 所以「完整历史」本来就是一次读取，没有理由把分页强加给只想恢复页面的调用方。
///
/// 与 `GET .../events` 合起来就是完整的页面恢复：这里给内容，事件流给状态与业务
/// 事件。
pub async fn transcript(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant_or_reject(&state, &headers) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let id = match parse_id(&id) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    match state.sessions.transcript(&tenant, &id).await {
        Ok(Some(context)) => Json(transcript_object(&id, &context)).into_response(),
        Ok(None) => not_found_kind("session"),
        Err(e) => map_sessions_error(&e),
    }
}

/// 历史 → 协议形状。
///
/// 形状刻意与官方的列表约定一致（`object: "list"` + `data`），但**没有** `has_more`
/// 或游标字段：一次就是全部，声明一个永远为假的 `has_more` 只会让调用方以为这里
/// 有分页可翻。
fn transcript_object(id: &SessionId, context: &ResolvedContext) -> Value {
    json!({
        "object": "list",
        "session_id": id.to_string(),
        "data": context.items,
    })
}
