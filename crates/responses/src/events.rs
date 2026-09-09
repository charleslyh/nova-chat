//! Stream event names aligned with the upstream protocol (D22).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::context::ResponseId;
use crate::shared::Attempt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResponseEventKind {
    #[serde(rename = "response.created")]
    Created,
    #[serde(rename = "response.in_progress")]
    InProgress,
    #[serde(rename = "response.output_text.delta")]
    OutputTextDelta,
    /// Reasoning / thinking text streamed by a reasoning model (DeepSeek-R1,
    /// o1, QwQ, …). Never an item and never fed back as context (a model does
    /// not read its own thinking, and D22 keeps `reasoning` out of the item
    /// subset); it is persisted on the response for re-render, but as a field,
    /// not as an output item.
    #[serde(rename = "response.reasoning_text.delta")]
    ReasoningTextDelta,
    #[serde(rename = "response.output_item.added")]
    OutputItemAdded,
    #[serde(rename = "response.output_item.done")]
    OutputItemDone,
    #[serde(rename = "response.function_call_arguments.delta")]
    FunctionCallArgumentsDelta,
    #[serde(rename = "response.function_call_arguments.done")]
    FunctionCallArgumentsDone,
    #[serde(rename = "response.content_part.added")]
    ContentPartAdded,
    #[serde(rename = "response.output_text.done")]
    OutputTextDone,
    #[serde(rename = "response.content_part.done")]
    ContentPartDone,
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
            ResponseEventKind::OutputTextDelta
                | ResponseEventKind::FunctionCallArgumentsDelta
                | ResponseEventKind::ReasoningTextDelta
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
            ResponseEventKind::ReasoningTextDelta => "response.reasoning_text.delta",
            ResponseEventKind::OutputItemAdded => "response.output_item.added",
            ResponseEventKind::OutputItemDone => "response.output_item.done",
            ResponseEventKind::FunctionCallArgumentsDelta => "response.function_call_arguments.delta",
            ResponseEventKind::FunctionCallArgumentsDone => "response.function_call_arguments.done",
            ResponseEventKind::ContentPartAdded => "response.content_part.added",
            ResponseEventKind::OutputTextDone => "response.output_text.done",
            ResponseEventKind::ContentPartDone => "response.content_part.done",
            ResponseEventKind::Completed => "response.completed",
            ResponseEventKind::Failed => "response.failed",
            ResponseEventKind::Incomplete => "response.incomplete",
        }
    }
}

/// An event a producer hands to the log, before it has a sequence number.
///
/// The sequence number is assigned by the `ResponseEventLog` implementation and
/// returned from `append` (INV-11). A producer therefore never supplies one:
/// this type has no `sequence_number` field, so there is nothing to invent, and
/// nothing for a backend to overwrite or trust. This mirrors
/// `ConversationStore::append_event`, whose input is a `ConversationEventKind`
/// and whose seq is likewise returned rather than passed in — the number is the
/// implementation's to assign, so it never appears on the append input.
///
/// Like [`ResponseEvent`], `response_id` and `attempt` are internal state and
/// never serialised; a cross-process carrier that must preserve them uses its
/// own wire type (see `adapters/mem/src/proto.rs`).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AppendEvent {
    #[serde(skip)]
    pub response_id: ResponseId,
    #[serde(rename = "type")]
    pub kind: ResponseEventKind,
    #[serde(skip)]
    pub attempt: Option<Attempt>,
    #[serde(flatten)]
    pub body: EventBody,
}

/// A single stored or re-read event, serialised to the OpenAI Responses wire
/// shape: `type`, `sequence_number`, plus kind-specific fields (`delta`,
/// `item`, `arguments`).
///
/// This is the form produced by reads and serialised to the SSE wire — **not**
/// the form handed to `append`. A producer builds an [`AppendEvent`]; the log
/// assigns the sequence number and yields this type back on read.
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EventBody {
    /// `response.output_text.delta` / `response.function_call_arguments.delta`.
    /// `content_index` is present only for output_text deltas.
    Delta {
        item_id: String,
        output_index: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        content_index: Option<u32>,
        delta: String,
    },
    /// `response.output_item.added` / `response.output_item.done`.
    Item { output_index: u32, item: Value },
    /// `response.function_call_arguments.done`.
    Arguments {
        output_index: u32,
        item_id: String,
        arguments: String,
    },
    /// `response.output_text.done`.
    Text {
        item_id: String,
        output_index: u32,
        content_index: u32,
        text: String,
    },
    /// `response.content_part.added` / `response.content_part.done`.
    Part {
        item_id: String,
        output_index: u32,
        content_index: u32,
        part: Value,
    },
    /// Lifecycle events carry the full response object as `response`.
    Response { response: Value },
}

impl AppendEvent {
    /// Lifecycle envelope (created / in_progress / completed / failed /
    /// incomplete), carrying the full response object.
    pub fn lifecycle(response_id: ResponseId, kind: ResponseEventKind, response: Value) -> Self {
        Self {
            response_id,
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
            kind,
            attempt: Some(attempt),
            body: EventBody::Response { response },
        }
    }

    /// A text fragment (`response.output_text.delta`).
    pub fn text_delta(
        response_id: ResponseId,
        attempt: Attempt,
        item_id: String,
        output_index: u32,
        content_index: u32,
        delta: impl Into<String>,
    ) -> Self {
        Self {
            response_id,
            kind: ResponseEventKind::OutputTextDelta,
            attempt: Some(attempt),
            body: EventBody::Delta {
                item_id,
                output_index,
                content_index: Some(content_index),
                delta: delta.into(),
            },
        }
    }

    /// A tool-argument fragment (`response.function_call_arguments.delta`).
    pub fn arguments_delta(
        response_id: ResponseId,
        attempt: Attempt,
        item_id: String,
        output_index: u32,
        delta: impl Into<String>,
    ) -> Self {
        Self {
            response_id,
            kind: ResponseEventKind::FunctionCallArgumentsDelta,
            attempt: Some(attempt),
            body: EventBody::Delta {
                item_id,
                output_index,
                content_index: None,
                delta: delta.into(),
            },
        }
    }

    /// A reasoning / thinking fragment (`response.reasoning_text.delta`).
    ///
    /// It carries no item id and no content index, because reasoning is not an
    /// output item (D22 keeps `reasoning` out of the subset). The stream carries
    /// it for live rendering; persistence happens on the stored response.
    pub fn reasoning_text_delta(
        response_id: ResponseId,
        attempt: Attempt,
        delta: impl Into<String>,
    ) -> Self {
        Self {
            response_id,
            kind: ResponseEventKind::ReasoningTextDelta,
            attempt: Some(attempt),
            body: EventBody::Delta {
                item_id: String::new(),
                output_index: 0,
                content_index: None,
                delta: delta.into(),
            },
        }
    }

    /// The completed text of an output_text part (`response.output_text.done`).
    pub fn output_text_done(
        response_id: ResponseId,
        attempt: Attempt,
        item_id: String,
        output_index: u32,
        content_index: u32,
        text: impl Into<String>,
    ) -> Self {
        Self {
            response_id,
            kind: ResponseEventKind::OutputTextDone,
            attempt: Some(attempt),
            body: EventBody::Text {
                item_id,
                output_index,
                content_index,
                text: text.into(),
            },
        }
    }

    /// A content-part boundary (`response.content_part.added` / `.done`).
    pub fn content_part(
        response_id: ResponseId,
        kind: ResponseEventKind,
        attempt: Attempt,
        item_id: String,
        output_index: u32,
        content_index: u32,
        part: Value,
    ) -> Self {
        Self {
            response_id,
            kind,
            attempt: Some(attempt),
            body: EventBody::Part {
                item_id,
                output_index,
                content_index,
                part,
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
            kind: ResponseEventKind::FunctionCallArgumentsDone,
            attempt: Some(attempt),
            body: EventBody::Arguments {
                output_index,
                item_id,
                arguments,
            },
        }
    }

    /// Attach the sequence number the log assigned, yielding the stored/wire
    /// form. Called by `ResponseEventLog` implementations on append; producers
    /// never do this themselves.
    pub fn with_seq(self, sequence_number: u64) -> ResponseEvent {
        ResponseEvent {
            response_id: self.response_id,
            sequence_number,
            kind: self.kind,
            attempt: self.attempt,
            body: self.body,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::NodeTag;

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
                ResponseEventKind::ReasoningTextDelta,
                "\"response.reasoning_text.delta\"",
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
        assert!(ResponseEventKind::ReasoningTextDelta.coalescible());
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
        let event = AppendEvent::lifecycle(
            ResponseId::new(NodeTag::parse("n1").unwrap()),
            ResponseEventKind::Created,
            serde_json::json!({
                "id": "resp_test",
                "object": "response",
                "status": "queued",
            }),
        )
        .with_seq(0);
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
        let event = AppendEvent::text_delta(id, Attempt(1), "msg_1".into(), 0, 0, "hello").with_seq(0);
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], "response.output_text.delta");
        assert_eq!(json["item_id"], "msg_1");
        assert_eq!(json["output_index"], 0);
        assert_eq!(json["content_index"], 0);
        assert_eq!(json["delta"], "hello");
        assert!(json.get("payload").is_none());
    }

    #[test]
    fn arguments_delta_has_no_content_index() {
        let id = ResponseId::new(NodeTag::parse("n1").unwrap());
        let event = AppendEvent::arguments_delta(id, Attempt(1), "call_1".into(), 0, "{}").with_seq(0);
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], "response.function_call_arguments.delta");
        assert_eq!(json["item_id"], "call_1");
        assert_eq!(json["output_index"], 0);
        assert_eq!(json["delta"], "{}");
        assert!(json.get("content_index").is_none());
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
        let event = AppendEvent::item(id, ResponseEventKind::OutputItemAdded, Attempt(1), 0, item).with_seq(0);
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
        let event = AppendEvent::arguments(
            id,
            Attempt(1),
            0,
            "call_1".into(),
            r#"{"city":"Paris"}"#.into(),
        )
        .with_seq(0);
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], "response.function_call_arguments.done");
        assert_eq!(json["output_index"], 0);
        assert_eq!(json["item_id"], "call_1");
        assert_eq!(json["arguments"], r#"{"city":"Paris"}"#);
        assert!(json.get("payload").is_none());
    }
}
