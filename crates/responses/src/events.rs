//! Stream event names and bodies, aligned with the upstream protocol (D22).
//!
//! # Why the fields are private
//!
//! An event's `kind` decides what its body must be: `response.created` carries a
//! response object, `response.output_text.delta` carries a delta. With both public
//! it was possible — and cheap — to build a `Created` event holding a `Delta` body,
//! producing a stream a client cannot interpret. Construction therefore goes
//! through the constructors below, one per legal pairing, and the pair is read back
//! through accessors.
//!
//! # Why the body is `untagged`
//!
//! [`crate::protocol`] forbids `untagged` for the *externally accepted* surface,
//! where it would swallow the location of a caller's mistake. This is the opposite
//! situation: the body is never parsed from caller input, only from a sibling
//! process's serialisation of a value this crate produced, and the variants are
//! distinguished by disjoint required fields. The alternative — a second tag beside
//! `kind` — would be a second source of truth for the same fact.

use serde::{Deserialize, Serialize};

use crate::identity::Attempt;
use crate::protocol::{ContentPart, ResponseItem, ResponseObject};
use crate::response::ResponseId;

/// Event names, exactly as they appear on the wire.
///
/// Each name is written **once**: the strum derive drives `as_str`, `Display`,
/// `FromStr` and the serde impls below, so there is no second list to keep in
/// step.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    strum::IntoStaticStr,
    strum::EnumString,
    strum::Display,
)]
pub enum ResponseEventKind {
    #[strum(serialize = "response.created")]
    Created,
    #[strum(serialize = "response.in_progress")]
    InProgress,
    #[strum(serialize = "response.output_text.delta")]
    OutputTextDelta,
    /// Reasoning / thinking text streamed by a reasoning model (DeepSeek-R1, o1,
    /// QwQ, …). Never an item and never fed back as context (a model does not read
    /// its own thinking, and D22 keeps `reasoning` out of the item subset); it is
    /// persisted on the response for re-render, but as a field, not an item.
    #[strum(serialize = "response.reasoning_text.delta")]
    ReasoningTextDelta,
    #[strum(serialize = "response.output_item.added")]
    OutputItemAdded,
    #[strum(serialize = "response.output_item.done")]
    OutputItemDone,
    #[strum(serialize = "response.function_call_arguments.delta")]
    FunctionCallArgumentsDelta,
    #[strum(serialize = "response.function_call_arguments.done")]
    FunctionCallArgumentsDone,
    #[strum(serialize = "response.content_part.added")]
    ContentPartAdded,
    #[strum(serialize = "response.content_part.done")]
    ContentPartDone,
    #[strum(serialize = "response.output_text.done")]
    OutputTextDone,
    #[strum(serialize = "response.completed")]
    Completed,
    #[strum(serialize = "response.failed")]
    Failed,
    #[strum(serialize = "response.incomplete")]
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
            ResponseEventKind::Completed | ResponseEventKind::Failed | ResponseEventKind::Incomplete
        )
    }

    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

impl Serialize for ResponseEventKind {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ResponseEventKind {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        raw.parse().map_err(serde::de::Error::custom)
    }
}

/// The kind-specific fields of a streaming event, matching the upstream event
/// objects. Flattened onto the event when serialised.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EventBody {
    /// `response.output_text.delta` / `.function_call_arguments.delta` /
    /// `.reasoning_text.delta`. `content_index` is present only for text deltas.
    Delta {
        item_id: String,
        output_index: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_index: Option<u32>,
        delta: String,
    },
    /// `response.output_item.added` / `.done`. Typed, not `Value`: an output item
    /// is a domain value, and going through `serde_json::to_value(..)` at the
    /// producer meant a serialisation failure silently became `null`.
    Item {
        output_index: u32,
        item: ResponseItem,
    },
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
    /// `response.content_part.added` / `.done`.
    Part {
        item_id: String,
        output_index: u32,
        content_index: u32,
        part: ContentPart,
    },
    /// Lifecycle events carry the full response object.
    Response { response: Box<ResponseObject> },
}

/// An event a producer hands to the log, before it has a sequence number.
///
/// The sequence number is assigned by the [`crate::ports::ResponseEventLog`]
/// implementation and returned from `append` (INV-11). A producer therefore never
/// supplies one: this type has no field for it, so there is nothing to invent and
/// nothing for a backend to overwrite or trust.
#[derive(Debug, Clone, PartialEq)]
pub struct AppendEvent {
    response_id: ResponseId,
    kind: ResponseEventKind,
    /// Fencing token. `None` on events emitted by a path that has already checked
    /// the fence (or has no attempt of its own, such as `created`).
    attempt: Option<Attempt>,
    body: EventBody,
}

impl AppendEvent {
    pub fn response_id(&self) -> &ResponseId {
        &self.response_id
    }

    pub fn kind(&self) -> ResponseEventKind {
        self.kind
    }

    pub fn attempt(&self) -> Option<Attempt> {
        self.attempt
    }

    pub fn body(&self) -> &EventBody {
        &self.body
    }

    /// Reassemble an event received over a carrier's own wire format.
    ///
    /// The only way to build an event from parts, and deliberately not part of the
    /// producer-facing API: it exists for adapters that must round-trip an event
    /// this crate produced, which is why it is allowed to pair any kind with any
    /// body — the pairing was already checked where the event was first built.
    pub fn from_parts(
        response_id: ResponseId,
        kind: ResponseEventKind,
        attempt: Option<Attempt>,
        body: EventBody,
    ) -> Self {
        Self {
            response_id,
            kind,
            attempt,
            body,
        }
    }

    /// Lifecycle envelope (created / completed / failed / incomplete), carrying
    /// the full response object.
    ///
    /// No attempt: the terminal transition has already been validated by the
    /// ledger, and re-checking the fence here would reject the very event that
    /// announces the transition, leaving the stream never terminated.
    pub fn lifecycle(
        response_id: ResponseId,
        kind: ResponseEventKind,
        response: ResponseObject,
    ) -> Self {
        Self {
            response_id,
            kind,
            attempt: None,
            body: EventBody::Response {
                response: Box::new(response),
            },
        }
    }

    /// A lifecycle envelope that still carries the fence — used for `in_progress`,
    /// the one lifecycle event emitted before a terminal transition has checked it.
    pub fn lifecycle_with_attempt(
        response_id: ResponseId,
        kind: ResponseEventKind,
        attempt: Attempt,
        response: ResponseObject,
    ) -> Self {
        Self {
            response_id,
            kind,
            attempt: Some(attempt),
            body: EventBody::Response {
                response: Box::new(response),
            },
        }
    }

    /// A text fragment (`response.output_text.delta`).
    pub fn text_delta(
        response_id: ResponseId,
        attempt: Attempt,
        item_id: impl Into<String>,
        output_index: u32,
        content_index: u32,
        delta: impl Into<String>,
    ) -> Self {
        Self {
            response_id,
            kind: ResponseEventKind::OutputTextDelta,
            attempt: Some(attempt),
            body: EventBody::Delta {
                item_id: item_id.into(),
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
        item_id: impl Into<String>,
        output_index: u32,
        delta: impl Into<String>,
    ) -> Self {
        Self {
            response_id,
            kind: ResponseEventKind::FunctionCallArgumentsDelta,
            attempt: Some(attempt),
            body: EventBody::Delta {
                item_id: item_id.into(),
                output_index,
                content_index: None,
                delta: delta.into(),
            },
        }
    }

    /// A reasoning / thinking fragment (`response.reasoning_text.delta`).
    ///
    /// It carries no item id and no content index, because reasoning is not an
    /// output item (D22 keeps it out of the subset). The stream carries it for live
    /// rendering; persistence happens on the stored response.
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
        item_id: impl Into<String>,
        output_index: u32,
        content_index: u32,
        text: impl Into<String>,
    ) -> Self {
        Self {
            response_id,
            kind: ResponseEventKind::OutputTextDone,
            attempt: Some(attempt),
            body: EventBody::Text {
                item_id: item_id.into(),
                output_index,
                content_index,
                text: text.into(),
            },
        }
    }

    /// A content-part boundary. `done` iff `done`, so the caller cannot pass a
    /// kind that has nothing to do with content parts.
    pub fn content_part(
        response_id: ResponseId,
        attempt: Attempt,
        done: bool,
        item_id: impl Into<String>,
        output_index: u32,
        content_index: u32,
        part: ContentPart,
    ) -> Self {
        Self {
            response_id,
            kind: if done {
                ResponseEventKind::ContentPartDone
            } else {
                ResponseEventKind::ContentPartAdded
            },
            attempt: Some(attempt),
            body: EventBody::Part {
                item_id: item_id.into(),
                output_index,
                content_index,
                part,
            },
        }
    }

    /// A whole output item. `done` iff `done`, for the same reason as
    /// [`Self::content_part`].
    pub fn item(
        response_id: ResponseId,
        attempt: Attempt,
        done: bool,
        output_index: u32,
        item: ResponseItem,
    ) -> Self {
        Self {
            response_id,
            kind: if done {
                ResponseEventKind::OutputItemDone
            } else {
                ResponseEventKind::OutputItemAdded
            },
            attempt: Some(attempt),
            body: EventBody::Item { output_index, item },
        }
    }

    /// Completed arguments for a function call.
    pub fn arguments(
        response_id: ResponseId,
        attempt: Attempt,
        output_index: u32,
        item_id: impl Into<String>,
        arguments: impl Into<String>,
    ) -> Self {
        Self {
            response_id,
            kind: ResponseEventKind::FunctionCallArgumentsDone,
            attempt: Some(attempt),
            body: EventBody::Arguments {
                output_index,
                item_id: item_id.into(),
                arguments: arguments.into(),
            },
        }
    }

    /// Attach the sequence number the log assigned, yielding the stored/read form.
    /// Called by [`crate::ports::ResponseEventLog`] implementations on append;
    /// producers never do this themselves.
    pub fn with_seq(self, sequence_number: u64) -> ResponseEvent {
        ResponseEvent {
            sequence_number,
            event: self,
        }
    }
}

/// A stored or re-read event: an [`AppendEvent`] plus the sequence number the log
/// gave it.
///
/// Composed rather than a second struct repeating the same four fields — the two
/// used to be near-copies kept in step by a hand-written conversion.
#[derive(Debug, Clone, PartialEq)]
pub struct ResponseEvent {
    sequence_number: u64,
    event: AppendEvent,
}

impl ResponseEvent {
    /// 0-based, contiguous within a single response (INV-11).
    pub fn sequence_number(&self) -> u64 {
        self.sequence_number
    }

    pub fn response_id(&self) -> &ResponseId {
        self.event.response_id()
    }

    pub fn kind(&self) -> ResponseEventKind {
        self.event.kind()
    }

    pub fn attempt(&self) -> Option<Attempt> {
        self.event.attempt()
    }

    pub fn body(&self) -> &EventBody {
        self.event.body()
    }

    /// The response object a lifecycle event carries, if this is one.
    pub fn response_object(&self) -> Option<&ResponseObject> {
        match self.body() {
            EventBody::Response { response } => Some(response),
            _ => None,
        }
    }

    /// Drop the sequence number, e.g. to re-append to another carrier.
    pub fn into_append(self) -> AppendEvent {
        self.event
    }
}

/// The SSE wire shape: `type`, `sequence_number`, and the body's own fields.
///
/// `response_id` and `attempt` are absent by construction rather than by
/// `skip`: the id lives in the SSE URL and the fence is internal concurrency
/// control, so neither has a place on the wire.
#[derive(Serialize)]
struct EventWire<'a> {
    #[serde(rename = "type")]
    kind: ResponseEventKind,
    sequence_number: u64,
    #[serde(flatten)]
    body: &'a EventBody,
}

impl Serialize for ResponseEvent {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        EventWire {
            kind: self.kind(),
            sequence_number: self.sequence_number,
            body: self.body(),
        }
        .serialize(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::NodeTag;
    use crate::protocol::ContentPart;

    fn id() -> ResponseId {
        ResponseId::new(NodeTag::parse("n1").unwrap())
    }

    fn stub() -> ResponseObject {
        ResponseObject::terminal_stub(&id(), crate::response::ResponseStatus::Queued)
    }

    #[test]
    fn event_names_have_exactly_one_definition() {
        // `as_str`, `Display`, serde and `FromStr` all come off the same derive, so
        // this checks the wiring rather than a hand-maintained second list.
        for (kind, name) in [
            (ResponseEventKind::Created, "response.created"),
            (ResponseEventKind::InProgress, "response.in_progress"),
            (ResponseEventKind::OutputTextDelta, "response.output_text.delta"),
            (
                ResponseEventKind::ReasoningTextDelta,
                "response.reasoning_text.delta",
            ),
            (ResponseEventKind::OutputItemAdded, "response.output_item.added"),
            (ResponseEventKind::OutputItemDone, "response.output_item.done"),
            (
                ResponseEventKind::FunctionCallArgumentsDelta,
                "response.function_call_arguments.delta",
            ),
            (
                ResponseEventKind::FunctionCallArgumentsDone,
                "response.function_call_arguments.done",
            ),
            (ResponseEventKind::ContentPartAdded, "response.content_part.added"),
            (ResponseEventKind::ContentPartDone, "response.content_part.done"),
            (ResponseEventKind::OutputTextDone, "response.output_text.done"),
            (ResponseEventKind::Completed, "response.completed"),
            (ResponseEventKind::Failed, "response.failed"),
            (ResponseEventKind::Incomplete, "response.incomplete"),
        ] {
            assert_eq!(kind.as_str(), name);
            assert_eq!(kind.to_string(), name);
            assert_eq!(serde_json::to_string(&kind).unwrap(), format!("\"{name}\""));
            assert_eq!(
                serde_json::from_str::<ResponseEventKind>(&format!("\"{name}\"")).unwrap(),
                kind
            );
        }
    }

    #[test]
    fn unknown_event_names_are_rejected() {
        assert!(serde_json::from_str::<ResponseEventKind>(r#""response.invented""#).is_err());
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
        for kind in [
            ResponseEventKind::Completed,
            ResponseEventKind::Failed,
            ResponseEventKind::Incomplete,
        ] {
            assert!(kind.is_terminal(), "{kind:?}");
        }
        for kind in [
            ResponseEventKind::Created,
            ResponseEventKind::InProgress,
            ResponseEventKind::OutputTextDelta,
            ResponseEventKind::OutputItemAdded,
            ResponseEventKind::OutputItemDone,
            ResponseEventKind::FunctionCallArgumentsDelta,
            ResponseEventKind::FunctionCallArgumentsDone,
        ] {
            assert!(!kind.is_terminal(), "{kind:?}");
        }
    }

    #[test]
    fn every_constructor_pairs_its_kind_with_a_body_that_matches() {
        // The property the private fields buy: there is no way to reach a state
        // where the two disagree, so this asserts the pairings rather than hoping
        // callers get them right.
        let a = Attempt(1);
        let cases = [
            (
                AppendEvent::lifecycle(id(), ResponseEventKind::Created, stub()),
                true,
            ),
            (
                AppendEvent::lifecycle_with_attempt(
                    id(),
                    ResponseEventKind::InProgress,
                    a,
                    stub(),
                ),
                true,
            ),
        ];
        for (event, is_response_body) in cases {
            assert_eq!(
                matches!(event.body(), EventBody::Response { .. }),
                is_response_body
            );
        }

        assert!(matches!(
            AppendEvent::text_delta(id(), a, "m", 0, 0, "x").body(),
            EventBody::Delta { .. }
        ));
        assert!(matches!(
            AppendEvent::item(id(), a, false, 0, ResponseItem::user_text("i")).body(),
            EventBody::Item { .. }
        ));
        assert!(matches!(
            AppendEvent::arguments(id(), a, 0, "c", "{}").body(),
            EventBody::Arguments { .. }
        ));
        assert!(matches!(
            AppendEvent::output_text_done(id(), a, "m", 0, 0, "t").body(),
            EventBody::Text { .. }
        ));
        assert!(matches!(
            AppendEvent::content_part(
                id(),
                a,
                false,
                "m",
                0,
                0,
                ContentPart::OutputText { text: String::new() }
            )
            .body(),
            EventBody::Part { .. }
        ));
    }

    #[test]
    fn done_flags_select_the_done_kind() {
        let a = Attempt(1);
        assert_eq!(
            AppendEvent::item(id(), a, false, 0, ResponseItem::user_text("i")).kind(),
            ResponseEventKind::OutputItemAdded
        );
        assert_eq!(
            AppendEvent::item(id(), a, true, 0, ResponseItem::user_text("i")).kind(),
            ResponseEventKind::OutputItemDone
        );
        let part = ContentPart::OutputText { text: String::new() };
        assert_eq!(
            AppendEvent::content_part(id(), a, false, "m", 0, 0, part.clone()).kind(),
            ResponseEventKind::ContentPartAdded
        );
        assert_eq!(
            AppendEvent::content_part(id(), a, true, "m", 0, 0, part).kind(),
            ResponseEventKind::ContentPartDone
        );
    }

    #[test]
    fn lifecycle_events_carry_the_object_and_no_fence() {
        let event = AppendEvent::lifecycle(id(), ResponseEventKind::Created, stub());
        assert_eq!(event.attempt(), None);
        let stored = event.with_seq(0);
        assert!(stored.response_object().is_some());

        let fenced =
            AppendEvent::lifecycle_with_attempt(id(), ResponseEventKind::InProgress, Attempt(2), stub());
        assert_eq!(fenced.attempt(), Some(Attempt(2)));
    }

    #[test]
    fn event_serialises_with_protocol_field_names() {
        let json = serde_json::to_value(
            AppendEvent::lifecycle(id(), ResponseEventKind::Created, stub()).with_seq(0),
        )
        .unwrap();
        assert_eq!(json["sequence_number"], 0);
        assert_eq!(json["type"], "response.created");
        assert_eq!(json["response"]["object"], "response");
        for internal in ["response_id", "attempt", "payload", "seq"] {
            assert!(json.get(internal).is_none(), "{internal} leaked: {json}");
        }
    }

    #[test]
    fn delta_events_serialise_with_a_delta_field() {
        let json = serde_json::to_value(
            AppendEvent::text_delta(id(), Attempt(1), "msg_1", 0, 0, "hello").with_seq(3),
        )
        .unwrap();
        assert_eq!(json["type"], "response.output_text.delta");
        assert_eq!(json["sequence_number"], 3);
        assert_eq!(json["item_id"], "msg_1");
        assert_eq!(json["output_index"], 0);
        assert_eq!(json["content_index"], 0);
        assert_eq!(json["delta"], "hello");
    }

    #[test]
    fn arguments_delta_has_no_content_index() {
        let json = serde_json::to_value(
            AppendEvent::arguments_delta(id(), Attempt(1), "call_1", 0, "{}").with_seq(0),
        )
        .unwrap();
        assert_eq!(json["type"], "response.function_call_arguments.delta");
        assert_eq!(json["item_id"], "call_1");
        assert_eq!(json["delta"], "{}");
        assert!(json.get("content_index").is_none());
    }

    #[test]
    fn item_events_serialise_the_item_itself() {
        let item = ResponseItem::FunctionCall {
            call_id: "call_1".into(),
            name: "get_weather".into(),
            arguments: String::new(),
            id: None,
            status: None,
        };
        let json =
            serde_json::to_value(AppendEvent::item(id(), Attempt(1), false, 0, item).with_seq(0))
                .unwrap();
        assert_eq!(json["type"], "response.output_item.added");
        assert_eq!(json["output_index"], 0);
        assert_eq!(json["item"]["type"], "function_call");
        assert_eq!(json["item"]["call_id"], "call_1");
    }

    #[test]
    fn arguments_events_serialise_with_output_index_and_item_id() {
        let json = serde_json::to_value(
            AppendEvent::arguments(id(), Attempt(1), 0, "call_1", r#"{"city":"Paris"}"#)
                .with_seq(0),
        )
        .unwrap();
        assert_eq!(json["type"], "response.function_call_arguments.done");
        assert_eq!(json["output_index"], 0);
        assert_eq!(json["item_id"], "call_1");
        assert_eq!(json["arguments"], r#"{"city":"Paris"}"#);
    }

    #[test]
    fn bodies_round_trip_through_a_carriers_own_wire_form() {
        // What `untagged` has to hold up for: every body must come back as itself
        // when a sibling process re-reads it.
        let a = Attempt(1);
        for event in [
            AppendEvent::lifecycle(id(), ResponseEventKind::Created, stub()),
            AppendEvent::text_delta(id(), a, "m", 1, 2, "x"),
            AppendEvent::arguments_delta(id(), a, "c", 1, "{"),
            AppendEvent::item(id(), a, true, 4, ResponseItem::assistant_text("o")),
            AppendEvent::arguments(id(), a, 0, "c", "{}"),
            AppendEvent::output_text_done(id(), a, "m", 0, 1, "t"),
            AppendEvent::content_part(
                id(),
                a,
                true,
                "m",
                0,
                1,
                ContentPart::OutputText { text: "t".into() },
            ),
        ] {
            let json = serde_json::to_string(event.body()).unwrap();
            let back: EventBody = serde_json::from_str(&json).unwrap();
            assert_eq!(&back, event.body(), "{json}");
        }
    }

    #[test]
    fn the_sequence_number_is_the_logs_to_assign() {
        let event = AppendEvent::text_delta(id(), Attempt(1), "m", 0, 0, "x");
        let stored = event.clone().with_seq(7);
        assert_eq!(stored.sequence_number(), 7);
        // And it can be handed back to another carrier without its number.
        assert_eq!(stored.into_append(), event);
    }
}
