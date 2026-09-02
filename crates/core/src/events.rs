//! Stream event names aligned with the upstream protocol (D22).

use serde::{Deserialize, Serialize};

use crate::ids::{Attempt, ResponseId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResponseEventKind {
    #[serde(rename = "response.created")]
    Created,
    #[serde(rename = "response.in_progress")]
    InProgress,
    #[serde(rename = "response.output_text.delta")]
    OutputTextDelta,
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
        matches!(self, ResponseEventKind::OutputTextDelta)
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
            ResponseEventKind::Completed => "response.completed",
            ResponseEventKind::Failed => "response.failed",
            ResponseEventKind::Incomplete => "response.incomplete",
        }
    }
}

/// A single event in one response's stream.
///
/// `payload` is free-form and may carry render-only material (tool progress,
/// reasoning summaries, UI hints). That is safe precisely because stored items
/// travel a **separate** write path (INV-48) — nothing here is ever replayed to
/// derive the stored output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseEvent {
    pub response_id: ResponseId,
    /// 0-based, contiguous within a single response (INV-11).
    pub sequence_number: u64,
    #[serde(rename = "type")]
    pub kind: ResponseEventKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<Attempt>,
    #[serde(default)]
    pub payload: String,
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
    fn only_delta_is_coalescible() {
        assert!(ResponseEventKind::OutputTextDelta.coalescible());
        for kind in [
            ResponseEventKind::Created,
            ResponseEventKind::InProgress,
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
    }

    #[test]
    fn event_serialises_with_protocol_field_names() {
        let event = ResponseEvent {
            response_id: ResponseId::new(NodeTag::parse("n1").unwrap()),
            sequence_number: 0,
            kind: ResponseEventKind::Created,
            attempt: None,
            payload: String::new(),
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["sequence_number"], 0);
        assert_eq!(json["type"], "response.created");
        assert!(json.get("seq").is_none(), "legacy field name must be gone");
    }
}
