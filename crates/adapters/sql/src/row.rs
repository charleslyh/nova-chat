//! Row ↔ domain mapping.
//!
//! Kept in one place so the ledger and the context store cannot decode the
//! shared table differently.

use std::collections::BTreeMap;

use nova_responses_core::{
    AgentId, Attempt, ConversationId, IdempotencyKey, NodeTag, ResponseId, ResponseItem,
    ResponseStatus, SessionId, StoredResponse, TenantId, Usage,
};
use serde_json::Value;
use sqlx::postgres::PgRow;
use sqlx::Row;

use crate::error::SqlError;

pub(crate) fn status_to_str(status: ResponseStatus) -> &'static str {
    status.as_str()
}

pub(crate) fn status_from_str(raw: &str) -> Result<ResponseStatus, SqlError> {
    Ok(match raw {
        "queued" => ResponseStatus::Queued,
        "in_progress" => ResponseStatus::InProgress,
        "completed" => ResponseStatus::Completed,
        "failed" => ResponseStatus::Failed,
        "incomplete" => ResponseStatus::Incomplete,
        "cancelled" => ResponseStatus::Cancelled,
        other => return Err(SqlError::Decode(format!("unknown status `{other}`"))),
    })
}

pub(crate) fn items_from_json(value: Value) -> Result<Vec<ResponseItem>, SqlError> {
    serde_json::from_value(value).map_err(|e| SqlError::Decode(format!("items: {e}")))
}

pub(crate) fn items_to_json(items: &[ResponseItem]) -> Value {
    serde_json::to_value(items).unwrap_or_else(|_| Value::Array(vec![]))
}

pub(crate) fn usage_from_json(value: Value) -> Usage {
    serde_json::from_value(value).unwrap_or_default()
}

pub(crate) fn usage_to_json(usage: &Usage) -> Value {
    serde_json::to_value(usage).unwrap_or_else(|_| Value::Object(Default::default()))
}

/// Partial usage is keyed by attempt so repeated aborts accumulate rather than
/// overwrite (INV-51).
pub(crate) type PartialUsage = BTreeMap<String, Usage>;

pub(crate) fn partial_usage_from_json(value: Value) -> PartialUsage {
    serde_json::from_value(value).unwrap_or_default()
}

pub(crate) fn partial_usage_to_json(map: &PartialUsage) -> Value {
    serde_json::to_value(map).unwrap_or_else(|_| Value::Object(Default::default()))
}

pub(crate) fn total_usage(base: Usage, partial: &PartialUsage) -> Usage {
    partial.values().fold(base, |acc, u| acc.add(*u))
}

/// Decode an optional id column, keeping the column name in the error so a
/// mis-mapped column is identifiable from the message alone.
fn optional_id<T, F>(
    raw: Option<String>,
    column: &str,
    parse: F,
) -> Result<Option<T>, SqlError>
where
    F: Fn(&str) -> Result<T, nova_responses_core::IdError>,
{
    match raw {
        None => Ok(None),
        Some(raw) => parse(&raw)
            .map(Some)
            .map_err(|e| SqlError::Decode(format!("{column}: {e}"))),
    }
}

pub(crate) fn record_from_row(row: &PgRow) -> Result<StoredResponse, SqlError> {
    let response_id: String = row.try_get("response_id")?;
    let previous: Option<String> = row.try_get("previous_response_id")?;
    let tenant_id: String = row.try_get("tenant_id")?;
    let node_tag: String = row.try_get("node_tag")?;
    let status: String = row.try_get("status")?;
    let owner: Option<String> = row.try_get("owner")?;

    Ok(StoredResponse {
        response_id: ResponseId::parse(&response_id)
            .map_err(|e| SqlError::Decode(format!("response_id: {e}")))?,
        previous_response_id: optional_id(previous, "previous_response_id", ResponseId::parse)?,
        conversation_id: optional_id(
            row.try_get("conversation_id")?,
            "conversation_id",
            ConversationId::parse,
        )?,
        session_id: optional_id(row.try_get("session_id")?, "session_id", SessionId::parse)?,
        tenant_id: TenantId::parse(&tenant_id)
            .map_err(|e| SqlError::Decode(format!("tenant_id: {e}")))?,
        model: row.try_get("model")?,
        instructions: row.try_get("instructions")?,
        input_items: items_from_json(row.try_get("input_items")?)?,
        output_items: items_from_json(row.try_get("output_items")?)?,
        status: status_from_str(&status)?,
        usage: usage_from_json(row.try_get("usage")?),
        created_at_ms: row.try_get::<i64, _>("created_at_ms")? as u64,
        completed_at_ms: row
            .try_get::<Option<i64>, _>("completed_at_ms")?
            .map(|v| v as u64),
        stored: row.try_get("stored")?,
        expires_at_ms: row
            .try_get::<Option<i64>, _>("expires_at_ms")?
            .map(|v| v as u64),
        integrity: row.try_get("integrity")?,
        integrity_alg: row.try_get("integrity_alg")?,
        node_tag: NodeTag::parse(&node_tag)
            .map_err(|e| SqlError::Decode(format!("node_tag: {e}")))?,
        idempotency_key: row
            .try_get::<Option<String>, _>("idempotency_key")?
            .map(IdempotencyKey),
        owner: match owner {
            None => None,
            Some(raw) => Some(AgentId(
                raw.parse()
                    .map_err(|e| SqlError::Decode(format!("owner: {e}")))?,
            )),
        },
        attempt: Attempt(row.try_get::<i64, _>("attempt")? as u64),
        context: items_from_json(row.try_get("context")?)?,
        context_depth: row.try_get::<i64, _>("context_depth")? as usize,
    })
}

/// Columns every full-record query must select, so `record_from_row` always
/// finds what it needs.
pub(crate) const RECORD_COLUMNS: &str = "response_id, previous_response_id, tenant_id, model, \
     status, stored, node_tag, attempt, owner, idempotency_key, instructions, \
     input_items, output_items, usage, integrity, integrity_alg, \
     created_at_ms, completed_at_ms, expires_at_ms, context, context_depth, \
     conversation_id, session_id";
