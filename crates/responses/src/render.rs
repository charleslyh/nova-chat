//! Protocol rendering: a [`ResponseRecord`] → the OpenAI Responses wire object.
//!
//! Rendering lives here, not on the domain record, so the domain stays free of
//! the wire shape (`"object": "response"`, `created_at` in seconds, …). The
//! record is pure data; this module turns it into what leaves the node.

use serde_json::Value;

use crate::context::ResponseRecord;
use crate::protocol::ResponseItem;

/// The OpenAI-shaped response object for a response whose output is known,
/// embedded in lifecycle events and returned by `GET`. Only protocol fields
/// are exposed — the internal bookkeeping (`tenant_id`, `attempt`, …) never
/// leaves the node.
///
/// `output_items` is supplied by the caller rather than read from the record:
/// the record holds no output (output lives in the conversation snapshot and
/// the event stream), so the renderer passes in what it reconstructed.
pub fn response_object(record: &ResponseRecord, output_items: &[ResponseItem]) -> Value {
    serde_json::json!({
        "id": record.response_id.to_string(),
        "object": "response",
        "created_at": record.created_at_ms / 1000,
        "status": record.status.as_str(),
        "model": record.model,
        "previous_response_id": record.previous_response_id.as_ref().map(|v| v.to_string()),
        "conversation": record
            .conversation_id
            .as_ref()
            .map(|id| serde_json::json!({ "id": id.to_string() })),
        "instructions": record.instructions,
        "store": record.stored,
        "input": record.input_items,
        "output": output_items,
        "reasoning": record.reasoning,
        "usage": {
            "input_tokens": record.usage.input_tokens,
            "output_tokens": record.usage.output_tokens,
            "total_tokens": record.usage.total_tokens(),
        },
    })
}
