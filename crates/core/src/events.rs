//! Stream event names aligned with the upstream protocol (D22).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids::{Attempt, ResponseId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResponseEventKind {
    #[serde(rename = "response.created")]
    Created,
    #[serde(rename = "response.in_progress")]
    InProgress,
    #[serde(rename = "response.output_text.delta")]
    OutputTextDelta,
    #[serde(rename = "response.output_item.added")]
    OutputItemAdded,
    #[serde(rename = "response.output_item.done")]
    OutputItemDone,
    #[serde(rename = "response.function_call_arguments.delta")]
    FunctionCallArgumentsDelta,
    #[serde(rename = "response.function_call_arguments.done")]
    FunctionCallArgumentsDone,
    #[serde(rename = "response.completed")]
    Completed,
    #[serde(rename = "response.failed")]
    Failed,
    #[serde(rename = "response.incomplete")]
    Incomplete,
}

impl ResponseEventKind {
    /// INV-16: coalescible events may drop intermediate states; envelope events
    /// never may.
    pub fn coalescible(self) -> bool {
        matches!(
            self,
            ResponseEventKind::OutputTextDelta | ResponseEventKind::FunctionCallArgumentsDelta
        )
    }

    /// Terminal events start the retention window (INV-40).
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            ResponseEventKind::Completed
                | ResponseEventKind::Failed
                | ResponseEventKind::Incomplete
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ResponseEventKind::Created => "response.created",
            ResponseEventKind::InProgress => "response.in_progress",
            ResponseEventKind::OutputTextDelta => "response.output_text.delta",
            ResponseEventKind::OutputItemAdded => "response.output_item.added",
            ResponseEventKind::OutputItemDone => "response.output_item.done",
            ResponseEventKind::FunctionCallArgumentsDelta => "response.function_call_arguments.delta",
            ResponseEventKind::FunctionCallArgumentsDone => "response.function_call_arguments.done",
            ResponseEventKind::Completed => "response.completed",
            ResponseEventKind::Failed => "response.failed",
            ResponseEventKind::Incomplete => "response.incomplete",
        }
    }
}

/// A single event in one response's stream, serialised to the OpenAI Responses
/// wire shape: `type`, `sequence_number`, plus kind-specific fields (`delta`,
/// `item`, `arguments`). `response_id` and `attempt` never leave the process.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ResponseEvent {
    /// Not on the wire: the response id lives in the SSE URL.
    #[serde(skip)]
    pub response_id: ResponseId,
    /// 0-based, contiguous within a single response (INV-11).
    pub sequence_number: u64,
    #[serde(rename = "type")]
    pub kind: ResponseEventKind,
    /// Not on the wire: the attempt fence is an internal concurrency control.
    #[serde(skip)]
    pub attempt: Option<Attempt>,
    /// Kind-specific fields, flattened onto the event object.
    #[serde(flatten)]
    pub body: EventBody,
}

/// The kind-specific fields of a streaming event, matching the OpenAI Responses
/// event objects.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum EventBody {
    /// `response.output_text.delta` / `response.function_call_arguments.delta`.
    Delta { item_id: String, delta: String },
    /// `response.output_item.added` / `response.output_item.done`.
    Item { output_index: u32, item: Value },
    /// `response.function_call_arguments.done`.
    Arguments {
        output_index: u32,
        item_id: String,
        arguments: String,
    },
    /// Lifecycle events carry the full response object as `response`.
    Response { response: Value },
    /// No kind-specific fields. Kept for tests that construct bare events.
    Empty {},
}

impl ResponseEvent {
    /// Lifecycle envelope (created / in_progress / completed / failed /
    /// incomplete), carrying the full response object.
    pub fn lifecycle(response_id: ResponseId, kind: ResponseEventKind, response: Value) -> Self {
        Self {
            response_id,
            sequence_number: 0,
            kind,
            attempt: None,
            body: EventBody::Response { response },
        }
    }

    /// A lifecycle envelope that still carries the attempt fence — used for
    /// `in_progress`, the one lifecycle event emitted before a terminal
    /// transition has already checked the fence.
    pub fn lifecycle_with_attempt(
        response_id: ResponseId,
        kind: ResponseEventKind,
        attempt: Attempt,
        response: Value,
    ) -> Self {
        Self {
            response_id,
            sequence_number: 0,
            kind,
            attempt: Some(attempt),
            body: EventBody::Response { response },
        }
    }

    /// A text or arguments fragment.
    pub fn delta(
        response_id: ResponseId,
        kind: ResponseEventKind,
        attempt: Attempt,
        item_id: String,
        delta: impl Into<String>,
    ) -> Self {
        Self {
            response_id,
            sequence_number: 0,
            kind,
            attempt: Some(attempt),
            body: EventBody::Delta {
                item_id,
                delta: delta.into(),
            },
        }
    }

    /// A whole output item.
    pub fn item(
        response_id: ResponseId,
        kind: ResponseEventKind,
        attempt: Attempt,
        output_index: u32,
        item: Value,
    ) -> Self {
        Self {
            response_id,
            sequence_number: 0,
            kind,
            attempt: Some(attempt),
            body: EventBody::Item { output_index, item },
        }
    }

    /// Completed arguments for a function call.
    pub fn arguments(
        response_id: ResponseId,
        attempt: Attempt,
        output_index: u32,
        item_id: String,
        arguments: String,
    ) -> Self {
        Self {
            response_id,
            sequence_number: 0,
            kind: ResponseEventKind::FunctionCallArgumentsDone,
            attempt: Some(attempt),
            body: EventBody::Arguments {
                output_index,
                item_id,
                arguments,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::NodeTag;

    #[test]
    fn event_names_match_upstream() {
        let pairs = [
            (ResponseEventKind::Created, "\"response.created\""),
            (ResponseEventKind::InProgress, "\"response.in_progress\""),
            (
                ResponseEventKind::OutputTextDelta,
                "\"response.output_text.delta\"",
            ),
            (
                ResponseEventKind::OutputItemAdded,
                "\"response.output_item.added\"",
            ),
            (
                ResponseEventKind::OutputItemDone,
                "\"response.output_item.done\"",
            ),
            (
                ResponseEventKind::FunctionCallArgumentsDelta,
                "\"response.function_call_arguments.delta\"",
            ),
            (
                ResponseEventKind::FunctionCallArgumentsDone,
                "\"response.function_call_arguments.done\"",
            ),
            (ResponseEventKind::Completed, "\"response.completed\""),
            (ResponseEventKind::Failed, "\"response.failed\""),
            (ResponseEventKind::Incomplete, "\"response.incomplete\""),
        ];
        for (kind, json) in pairs {
            assert_eq!(serde_json::to_string(&kind).unwrap(), json);
            assert_eq!(
                serde_json::from_str::<ResponseEventKind>(json).unwrap(),
                kind
            );
            assert_eq!(format!("\"{}\"", kind.as_str()), json);
        }
    }

    #[test]
    fn only_incremental_events_are_coalescible() {
        assert!(ResponseEventKind::OutputTextDelta.coalescible());
        assert!(ResponseEventKind::FunctionCallArgumentsDelta.coalescible());
        for kind in [
            ResponseEventKind::Created,
            ResponseEventKind::InProgress,
            ResponseEventKind::OutputItemAdded,
            ResponseEventKind::OutputItemDone,
            ResponseEventKind::FunctionCallArgumentsDone,
            ResponseEventKind::Completed,
            ResponseEventKind::Failed,
            ResponseEventKind::Incomplete,
        ] {
            assert!(!kind.coalescible(), "{kind:?} must not be coalescible");
        }
    }

    #[test]
    fn terminal_set_is_exactly_the_three_end_states() {
        assert!(ResponseEventKind::Completed.is_terminal());
        assert!(ResponseEventKind::Failed.is_terminal());
        assert!(ResponseEventKind::Incomplete.is_terminal());
        assert!(!ResponseEventKind::Created.is_terminal());
        assert!(!ResponseEventKind::InProgress.is_terminal());
        assert!(!ResponseEventKind::OutputTextDelta.is_terminal());
        assert!(!ResponseEventKind::OutputItemAdded.is_terminal());
        assert!(!ResponseEventKind::OutputItemDone.is_terminal());
        assert!(!ResponseEventKind::FunctionCallArgumentsDelta.is_terminal());
        assert!(!ResponseEventKind::FunctionCallArgumentsDone.is_terminal());
    }

    #[test]
    fn event_serialises_with_protocol_field_names() {
        let event = ResponseEvent::lifecycle(
            ResponseId::new(NodeTag::parse("n1").unwrap()),
            ResponseEventKind::Created,
            serde_json::json!({
                "id": "resp_test",
                "object": "response",
                "status": "queued",
            }),
        );
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["sequence_number"], 0);
        assert_eq!(json["type"], "response.created");
        assert!(json.get("seq").is_none(), "legacy field name must be gone");
        assert!(
            json.get("response_id").is_none(),
            "internal id must not leak onto the wire: {json}"
        );
        assert!(
            json.get("attempt").is_none(),
            "internal fence must not leak onto the wire: {json}"
        );
        assert!(
            json.get("payload").is_none(),
            "legacy generic `payload` field must be gone: {json}"
        );
        // Lifecycle events carry the full response object as `response`.
        assert_eq!(json["response"]["id"], "resp_test");
        assert_eq!(json["response"]["status"], "queued");
    }

    #[test]
    fn delta_events_serialise_with_a_delta_field() {
        let id = ResponseId::new(NodeTag::parse("n1").unwrap());
        let event = ResponseEvent::delta(
            id,
            ResponseEventKind::OutputTextDelta,
            Attempt(1),
            "msg_1".into(),
            "hello",
        );
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], "response.output_text.delta");
        assert_eq!(json["item_id"], "msg_1");
        assert_eq!(json["delta"], "hello");
        assert!(json.get("payload").is_none());
    }

    #[test]
    fn item_events_serialise_with_output_index_and_nested_item() {
        let id = ResponseId::new(NodeTag::parse("n1").unwrap());
        let item = serde_json::json!({
            "type": "function_call",
            "call_id": "call_1",
            "name": "get_weather",
            "arguments": ""
        });
        let event = ResponseEvent::item(id, ResponseEventKind::OutputItemAdded, Attempt(1), 0, item);
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], "response.output_item.added");
        assert_eq!(json["output_index"], 0);
        assert_eq!(json["item"]["type"], "function_call");
        assert_eq!(json["item"]["call_id"], "call_1");
        assert!(json.get("payload").is_none());
    }

    #[test]
    fn arguments_events_serialise_with_output_index_and_item_id() {
        let id = ResponseId::new(NodeTag::parse("n1").unwrap());
        let event = ResponseEvent::arguments(
            id,
            Attempt(1),
            0,
            "call_1".into(),
            r#"{"city":"Paris"}"#.into(),
        );
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], "response.function_call_arguments.done");
        assert_eq!(json["output_index"], 0);
        assert_eq!(json["item_id"], "call_1");
        assert_eq!(json["arguments"], r#"{"city":"Paris"}"#);
        assert!(json.get("payload").is_none());
    }
}
