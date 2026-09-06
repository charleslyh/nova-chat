//! `/v1/conversations` — create, retrieve, update metadata, delete (D27).
//!
//! 这里是**接入层**：协议解析、租户鉴权、HTTP 翻译、`object` 字段渲染。业务编排
//! 委托给 `service` 层，本文件不直接操作端口。
//!
//! 官方的 `items` 子资源没有实现：容器只是指向响应链尾的指针，没有属于自己的条目
//! 可增删。读历史走会话层的 transcript，一次返回全部。

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use nova_responses_core::protocol::{CreateConversationRequest, UpdateConversationRequest};
use nova_responses_core::{Conversation, ConversationId};
use serde_json::{json, Value};

use crate::error::{bad_request, map_conversation_error, not_found_kind};
use crate::routes::shared::{parse_or_not_found, tenant_or_reject};
use crate::state::AppState;

/// 领域对象 → 协议形状。
///
/// `object` 这类展示常量只在这一层产生：领域类型里不带它，否则渲染细节会渗进领域。
/// `last_response_id` 也不外泄——它是我方的实现手段，官方的 conversation 对象没有
/// 这个字段，泄露出去会让调用方依赖一个官方协议里不存在的东西。
fn conversation_object(conversation: &Conversation) -> Value {
    json!({
        "id": conversation.id.to_string(),
        "object": "conversation",
        "created_at": conversation.created_at_ms / 1000,
        "metadata": conversation.metadata,
    })
}

fn parse_id(raw: &str) -> Result<ConversationId, Response> {
    parse_or_not_found(ConversationId::parse(raw), "conversation")
}

/// POST /v1/conversations
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

    let request: CreateConversationRequest = match serde_json::from_value(raw) {
        Ok(req) => req,
        // 未知字段一律 400，与生成入口同一姿态（INV-50）。
        Err(e) => return bad_request("invalid_request", e.to_string()),
    };
    if let Err(e) = request.validate() {
        return bad_request("invalid_request", e.to_string());
    }

    match state
        .conversations
        .create(&tenant, request.metadata())
        .await
    {
        Ok(conversation) => {
            (StatusCode::OK, Json(conversation_object(&conversation))).into_response()
        }
        Err(e) => map_conversation_error(&e),
    }
}

/// GET /v1/conversations/{id}
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

    match state.conversations.retrieve(&tenant, &id).await {
        Ok(Some(conversation)) => Json(conversation_object(&conversation)).into_response(),
        Ok(None) => not_found_kind("conversation"),
        Err(e) => map_conversation_error(&e),
    }
}

/// POST /v1/conversations/{id}
///
/// 官方用 POST 更新，不是 PATCH/PUT，所以与 GET/DELETE 共用同一路径。
pub async fn update(
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

    let request: UpdateConversationRequest = match serde_json::from_value(raw) {
        Ok(req) => req,
        Err(e) => return bad_request("invalid_request", e.to_string()),
    };
    if let Err(e) = request.validate() {
        return bad_request("invalid_request", e.to_string());
    }

    match state
        .conversations
        .update_metadata(&tenant, &id, request.metadata())
        .await
    {
        Ok(conversation) => Json(conversation_object(&conversation)).into_response(),
        Err(e) => map_conversation_error(&e),
    }
}

/// DELETE /v1/conversations/{id}
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

    match state.conversations.delete(&tenant, &id).await {
        Ok(true) => Json(json!({
            "id": id.to_string(),
            "object": "conversation.deleted",
            "deleted": true,
        }))
        .into_response(),
        Ok(false) => not_found_kind("conversation"),
        Err(e) => map_conversation_error(&e),
    }
}

fn draining() -> Response {
    crate::error::api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "draining",
        "node is shutting down; retry against the service",
    )
}
