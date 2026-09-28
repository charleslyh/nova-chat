//! `/v1/conversations` — create, retrieve, update metadata, delete (D27).
//!
//! 这里是**接入层**：协议解析、租户鉴权、HTTP 翻译、`object` 字段渲染。业务编排委托给
//! 能力层，本文件不直接操作端口（除 SSE：流本身就是传输）。
//!
//! 官方 `items` 子资源只实现**读**（`GET …/items`，D35）：条目来自会话物化快照，按位置派生
//! 稳定 id，分页语义与官方一致。增删（`POST/DELETE …/items`）仍不实现——快照只追加，条目
//! 级改写会让已提交的上下文与账本各说各话。transcript 保留为一次取全量的便捷形状。

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use nova_responses::protocol::{
    AppendBusinessEventRequest, ConversationMetadataRequest, MetadataValue, ResponseItem,
};
use nova_responses::{Conversation, ConversationEventKind, ConversationId, ResolvedContext};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{bad_request, map_conversation_error, not_found_kind};
use crate::routes::shared::{parse_or_not_found, tenant_or_reject, Reject};
use crate::sse::{open_conversation_stream, resolve_cursor};
use crate::state::AppState;

/// 领域对象 → 协议形状。
///
/// `object` 这类展示常量只在这一层产生：领域类型里不带它，否则渲染细节会渗进领域。
/// `last_response_id` 也不外泄——它是我方的实现手段，官方的 conversation 对象没有这个
/// 字段，泄露出去会让调用方依赖一个官方协议里不存在的东西。
#[derive(Debug, Serialize)]
struct ConversationObject {
    id: String,
    object: &'static str,
    created_at: u64,
    metadata: std::collections::BTreeMap<String, MetadataValue>,
    /// 自建扩展：轮次是否在途，供各端在生成期间禁用输入（D28）。官方对象没有这两个字段，
    /// 所以只在列表形状里出现，检索时省略。
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_response_id: Option<String>,
}

impl ConversationObject {
    /// 官方形状。
    fn new(conversation: &Conversation) -> Self {
        Self {
            id: conversation.id.to_string(),
            object: "conversation",
            created_at: conversation.created_at_ms / 1000,
            metadata: conversation.metadata.clone(),
            status: None,
            active_response_id: None,
        }
    }

    /// 列表形状：官方字段 + 在途状态。
    fn with_activity(conversation: &Conversation) -> Self {
        Self {
            status: Some(if conversation.active_response_id.is_some() {
                "busy"
            } else {
                "idle"
            }),
            active_response_id: conversation
                .active_response_id
                .as_ref()
                .map(|id| id.to_string()),
            ..Self::new(conversation)
        }
    }
}

#[derive(Debug, Serialize)]
struct DeletedObject {
    id: String,
    object: &'static str,
    deleted: bool,
}

#[derive(Debug, Serialize)]
struct AppendedEvent {
    object: &'static str,
    conversation_id: String,
    seq: u64,
}

/// 一次返回全部历史：没有 `has_more`，也就没有游标可以过期。
#[derive(Debug, Serialize)]
struct TranscriptObject {
    object: &'static str,
    conversation_id: String,
    data: Vec<TranscriptEntry>,
}

/// 一条历史条目：reasoning 块与它所属的 item 一起出现。
///
/// 曾经是两个必须等长的数组，靠 `zip` 交错——长度不一致时会静默丢掉整段历史，而这正是
/// 上游某个生产者的既有行为。现在结构上不可能错位。
#[derive(Debug, Serialize)]
#[serde(untagged)]
enum TranscriptEntry {
    Reasoning {
        #[serde(rename = "type")]
        kind: &'static str,
        text: String,
    },
    Item(ResponseItem),
}

fn parse_id(raw: &str) -> Result<ConversationId, Reject> {
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
        Err(resp) => return *resp,
    };
    if !state.is_accepting() {
        return draining();
    }

    let request: ConversationMetadataRequest = match serde_json::from_value(raw) {
        Ok(req) => req,
        // 未知字段一律 400，与生成入口同一姿态（INV-50）。
        Err(e) => return bad_request("invalid_request", e.to_string()),
    };
    if let Err(e) = request.validate(&state.responses_cfg().limits) {
        return bad_request("invalid_request", e.to_string());
    }

    match state.conversations.create(&tenant, request.metadata()).await {
        Ok(conversation) => (
            StatusCode::OK,
            Json(ConversationObject::new(&conversation)),
        )
            .into_response(),
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
        Err(resp) => return *resp,
    };
    let id = match parse_id(&id) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };

    match state.conversations.retrieve(&tenant, &id).await {
        Ok(Some(conversation)) => Json(ConversationObject::new(&conversation)).into_response(),
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
        Err(resp) => return *resp,
    };
    if !state.is_accepting() {
        return draining();
    }
    let id = match parse_id(&id) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };

    let request: ConversationMetadataRequest = match serde_json::from_value(raw) {
        Ok(req) => req,
        Err(e) => return bad_request("invalid_request", e.to_string()),
    };
    if let Err(e) = request.validate(&state.responses_cfg().limits) {
        return bad_request("invalid_request", e.to_string());
    }

    match state
        .conversations
        .update_metadata(&tenant, &id, request.metadata())
        .await
    {
        Ok(conversation) => Json(ConversationObject::new(&conversation)).into_response(),
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
        Err(resp) => return *resp,
    };
    let id = match parse_id(&id) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };

    match state.conversations.delete(&tenant, &id).await {
        Ok(true) => Json(DeletedObject {
            id: id.to_string(),
            object: "conversation.deleted",
            deleted: true,
        })
        .into_response(),
        Ok(false) => not_found_kind("conversation"),
        Err(e) => map_conversation_error(&e),
    }
}

#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    #[serde(default)]
    pub starting_after: Option<u64>,
}

#[derive(Debug, Serialize)]
struct ListObject<T> {
    object: &'static str,
    data: Vec<T>,
}

/// GET /v1/conversations — list every conversation the tenant owns, newest first.
pub async fn list(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let tenant = match tenant_or_reject(&state, &headers) {
        Ok(t) => t,
        Err(resp) => return *resp,
    };

    match state.conversations.list(&tenant).await {
        Ok(conversations) => Json(ListObject {
            object: "list",
            data: conversations
                .iter()
                .map(ConversationObject::with_activity)
                .collect::<Vec<_>>(),
        })
        .into_response(),
        Err(e) => map_conversation_error(&e),
    }
}

/// GET /v1/conversations/{id}/events — subscribe, replaying durable history from the
/// cursor before continuing live (D28). No separate snapshot endpoint, so no snapshot can
/// go stale.
pub async fn events(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<EventsQuery>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant_or_reject(&state, &headers) {
        Ok(t) => t,
        Err(resp) => return *resp,
    };
    let id = match parse_id(&id) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };

    // `Last-Event-ID` wins over the query: it reflects what the client actually received,
    // whereas the query is whatever it was told on first connect.
    let last_event_id = headers.get("last-event-id").and_then(|v| v.to_str().ok());
    let cursor = resolve_cursor(q.starting_after, last_event_id);

    open_conversation_stream(
        state.conversation_events.clone(),
        tenant,
        id,
        cursor,
        state.cfg.conversation_events_page,
    )
    .await
}

/// POST /v1/conversations/{id}/events — append a business event, ordered in the same
/// sequence space and never entering model context.
pub async fn append_event(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(raw): Json<Value>,
) -> Response {
    let tenant = match tenant_or_reject(&state, &headers) {
        Ok(t) => t,
        Err(resp) => return *resp,
    };
    if !state.is_accepting() {
        return draining();
    }
    let id = match parse_id(&id) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };

    let request: AppendBusinessEventRequest = match serde_json::from_value(raw) {
        Ok(req) => req,
        Err(e) => return bad_request("invalid_request", e.to_string()),
    };
    if let Err(e) = request.validate(&state.responses_cfg().limits) {
        return bad_request("invalid_request", e.to_string());
    }

    match state
        .conversations
        .append_event(
            &tenant,
            &id,
            ConversationEventKind::Business {
                kind: request.kind,
                payload: request.payload,
            },
        )
        .await
    {
        Ok(seq) => (
            StatusCode::OK,
            Json(AppendedEvent {
                object: "conversation.event",
                conversation_id: id.to_string(),
                seq,
            }),
        )
            .into_response(),
        Err(e) => map_conversation_error(&e),
    }
}

/// GET /v1/conversations/{id}/transcript — the whole history in one call.
pub async fn transcript(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant_or_reject(&state, &headers) {
        Ok(t) => t,
        Err(resp) => return *resp,
    };
    let id = match parse_id(&id) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };

    // 直接读会话物化主快照：它本就是完整历史的单一权威来源。没有再套一层
    // `TranscriptError`——那个类型只有一个透明变体，包装的还是同一个错误。
    match state.conversations.read_snapshot(&tenant, &id).await {
        Ok(context) => Json(transcript_object(&id, &context)).into_response(),
        Err(e) => map_conversation_error(&e),
    }
}

/// History → protocol shape, interleaving `reasoning` blocks so a reopened page replays
/// in order.
fn transcript_object(id: &ConversationId, context: &ResolvedContext) -> TranscriptObject {
    let mut data = Vec::with_capacity(context.item_count());
    for entry in &context.entries {
        if let Some(text) = &entry.reasoning {
            data.push(TranscriptEntry::Reasoning {
                kind: "reasoning",
                text: text.clone(),
            });
        }
        data.push(TranscriptEntry::Item(entry.item.clone()));
    }
    TranscriptObject {
        object: "list",
        conversation_id: id.to_string(),
        data,
    }
}

/// Query of `GET /v1/conversations/{id}/items` (official shape).
#[derive(Debug, Deserialize)]
pub struct ItemsQuery {
    /// Page after this item id, in the requested order.
    #[serde(default)]
    pub after: Option<String>,
    /// 1..=100, default 20.
    #[serde(default)]
    pub limit: Option<i64>,
    /// `asc` or `desc` (default `desc`, as upstream).
    #[serde(default)]
    pub order: Option<String>,
    /// Accepted for compatibility; every field this service stores is already
    /// in the item, so it changes nothing.
    #[serde(default)]
    pub include: Option<Value>,
}

const ITEMS_DEFAULT_LIMIT: usize = 20;
const ITEMS_MAX_LIMIT: usize = 100;

/// The official list page.
#[derive(Debug, Serialize)]
struct ItemsPage {
    object: &'static str,
    data: Vec<Value>,
    first_id: Option<String>,
    last_id: Option<String>,
    has_more: bool,
}

/// GET /v1/conversations/{id}/items — the conversation's items, paged (D35).
pub async fn items(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<ItemsQuery>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant_or_reject(&state, &headers) {
        Ok(t) => t,
        Err(resp) => return *resp,
    };
    let id = match parse_id(&id) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };
    let limit = match q.limit {
        None => ITEMS_DEFAULT_LIMIT,
        Some(n) if (1..=ITEMS_MAX_LIMIT as i64).contains(&n) => n as usize,
        Some(n) => {
            return bad_request(
                "invalid_request",
                format!("limit must be between 1 and {ITEMS_MAX_LIMIT}, got {n}"),
            )
        }
    };
    let descending = match q.order.as_deref() {
        None | Some("desc") => true,
        Some("asc") => false,
        Some(other) => {
            return bad_request(
                "invalid_request",
                format!("order must be `asc` or `desc`, got `{other}`"),
            )
        }
    };

    match state.conversations.read_snapshot(&tenant, &id).await {
        Ok(context) => {
            match items_page(&context, q.after.as_deref(), limit, descending) {
                Some(page) => Json(page).into_response(),
                // An unknown cursor is a client error, not an empty page: a page
                // that silently restarts would duplicate what the caller has.
                None => bad_request("invalid_request", "`after` is not an item of this conversation"),
            }
        }
        Err(e) => map_conversation_error(&e),
    }
}

/// History → official items, each with a STABLE id.
///
/// The snapshot is append-only and most stored items carry no id (only an
/// assistant message gets one, and a multi-round chain can repeat it). An item
/// keeps its native id when it is the first occurrence; otherwise its id is
/// derived from its position (`item_{n}`). Position is stable because the
/// snapshot never reorders or drops entries, so a cursor stays valid across
/// later turns.
///
/// Reasoning, stored beside the item it preceded, is rendered as the official
/// `reasoning` item (`summary_text`) right before it.
fn conversation_items(context: &ResolvedContext) -> Vec<Value> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::with_capacity(context.item_count());
    let mut position = 0usize;
    let mut next_id = |native: Option<&str>, seen: &mut std::collections::HashSet<String>| {
        let id = match native {
            Some(n) if !n.is_empty() && !seen.contains(n) => n.to_owned(),
            _ => format!("item_{position}"),
        };
        position += 1;
        seen.insert(id.clone());
        id
    };
    for entry in &context.entries {
        if let Some(text) = &entry.reasoning {
            let id = next_id(None, &mut seen);
            out.push(serde_json::json!({
                "type": "reasoning",
                "id": id,
                "summary": [{ "type": "summary_text", "text": text }],
            }));
        }
        let native = match &entry.item {
            ResponseItem::Message { id, .. }
            | ResponseItem::FunctionCall { id, .. }
            | ResponseItem::FunctionCallOutput { id, .. }
            | ResponseItem::CustomToolCall { id, .. } => id.clone(),
        };
        let id = next_id(native.as_deref(), &mut seen);
        let mut value = serde_json::to_value(&entry.item).unwrap_or(Value::Null);
        if let Value::Object(map) = &mut value {
            map.insert("id".into(), Value::String(id));
        }
        out.push(value);
    }
    out
}

/// One page, or `None` when `after` names no item of this conversation.
fn items_page(
    context: &ResolvedContext,
    after: Option<&str>,
    limit: usize,
    descending: bool,
) -> Option<ItemsPage> {
    let mut all = conversation_items(context);
    if descending {
        all.reverse();
    }
    let start = match after {
        None => 0,
        Some(cursor) => all.iter().position(|item| item["id"] == cursor)? + 1,
    };
    let rest = &all[start.min(all.len())..];
    let data: Vec<Value> = rest.iter().take(limit).cloned().collect();
    let id_of = |v: &Value| v["id"].as_str().map(str::to_owned);
    Some(ItemsPage {
        object: "list",
        first_id: data.first().and_then(id_of),
        last_id: data.last().and_then(id_of),
        has_more: rest.len() > data.len(),
        data,
    })
}

fn draining() -> Response {
    crate::error::api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "draining",
        "node is shutting down; retry against the service",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use nova_responses::ContextEntry;

    fn history() -> ResolvedContext {
        ResolvedContext::new(
            vec![
                ContextEntry::new(ResponseItem::user_text("q1")),
                ContextEntry::with_reasoning(
                    ResponseItem::assistant_text("a1"),
                    Some("thinking".into()),
                ),
                ContextEntry::new(ResponseItem::user_text("q2")),
                ContextEntry::new(ResponseItem::assistant_text("a2")),
            ],
            2,
        )
    }

    fn ids(page: &ItemsPage) -> Vec<String> {
        page.data.iter().map(|v| v["id"].as_str().unwrap().to_owned()).collect()
    }

    #[test]
    fn every_item_gets_a_unique_id_and_reasoning_is_an_official_item() {
        let items = conversation_items(&history());
        // Four items plus the reasoning block before `a1`.
        assert_eq!(items.len(), 5);
        assert_eq!(items[1]["type"], "reasoning");
        assert_eq!(items[1]["summary"][0]["type"], "summary_text");
        assert_eq!(items[1]["summary"][0]["text"], "thinking");
        let unique: std::collections::HashSet<_> =
            items.iter().map(|v| v["id"].as_str().unwrap().to_owned()).collect();
        assert_eq!(unique.len(), items.len(), "ids must be unique: {items:?}");
    }

    #[test]
    fn ids_survive_later_turns() {
        // A cursor handed out on page one must still point at the same item after
        // the conversation grows — the snapshot only appends.
        let before = conversation_items(&history());
        let mut grown = history();
        grown.entries.push(ContextEntry::new(ResponseItem::user_text("q3")));
        let after = conversation_items(&grown);
        for (old, new) in before.iter().zip(&after) {
            assert_eq!(old["id"], new["id"]);
        }
    }

    #[test]
    fn a_page_is_ordered_bounded_and_resumable() {
        let context = history();
        let first = items_page(&context, None, 2, false).unwrap();
        assert!(first.has_more);
        assert_eq!(first.data[0]["content"][0]["text"], "q1");
        let second = items_page(&context, first.last_id.as_deref(), 10, false).unwrap();
        assert!(!second.has_more);
        let mut all = ids(&first);
        all.extend(ids(&second));
        assert_eq!(all.len(), 5, "pages must neither overlap nor skip");

        // Default order is newest first.
        let newest = items_page(&context, None, 1, true).unwrap();
        assert_eq!(newest.data[0]["content"][0]["text"], "a2");
    }

    #[test]
    fn an_unknown_cursor_is_refused_not_restarted() {
        assert!(items_page(&history(), Some("item_999"), 5, false).is_none());
    }

    #[test]
    fn a_transcript_keeps_every_item_even_when_only_some_have_reasoning() {
        // The parallel-array version zipped items against reasoning; a producer that
        // returned no reasoning at all silently emptied the whole transcript.
        let context = ResolvedContext::new(
            vec![
                ContextEntry::new(ResponseItem::user_text("q")),
                ContextEntry::with_reasoning(
                    ResponseItem::assistant_text("a"),
                    Some("thinking".into()),
                ),
                ContextEntry::new(ResponseItem::user_text("q2")),
            ],
            2,
        );
        let object = transcript_object(&ConversationId::new(), &context);
        // Three items plus one interleaved reasoning block.
        assert_eq!(object.data.len(), 4);
        let json = serde_json::to_value(&object).unwrap();
        assert_eq!(json["data"][1]["type"], "reasoning");
        assert_eq!(json["data"][1]["text"], "thinking");
        assert_eq!(json["data"][2]["type"], "message");
        assert_eq!(json["object"], "list");
    }

    #[test]
    fn a_transcript_with_no_reasoning_still_returns_its_items() {
        let context = ResolvedContext::from_items([ResponseItem::user_text("q")], 1);
        let object = transcript_object(&ConversationId::new(), &context);
        assert_eq!(object.data.len(), 1);
    }

    #[test]
    fn the_official_conversation_object_hides_our_extensions() {
        let conversation = Conversation::new(
            ConversationId::new(),
            nova_responses::TenantId::parse("t1").unwrap(),
            Default::default(),
            7_000,
        );
        let json = serde_json::to_value(ConversationObject::new(&conversation)).unwrap();
        assert_eq!(json["object"], "conversation");
        assert_eq!(json["created_at"], 7);
        // Neither our in-flight extension nor the tail pointer may leak here.
        for hidden in ["status", "active_response_id", "last_response_id"] {
            assert!(json.get(hidden).is_none(), "{hidden} leaked: {json}");
        }

        // The list shape does carry the in-flight state, which is what it exists for.
        let listed = serde_json::to_value(ConversationObject::with_activity(&conversation)).unwrap();
        assert_eq!(listed["status"], "idle");
    }
}
