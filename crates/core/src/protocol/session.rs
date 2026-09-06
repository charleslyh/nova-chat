//! Wire types for the self-hosted session endpoints (D26).
//!
//! These are ours, not upstream's, and they live under `/v1/sessions` so the two
//! surfaces never overlap. The validation posture differs from the compatibility
//! layer on exactly one point, deliberately:
//!
//! - **Envelopes stay closed.** `deny_unknown_fields`, bounded sizes, no
//!   `flatten` — the same rules as everywhere else (D22 ②).
//! - **Business payloads stay opaque.** The service cannot know the caller's
//!   domain, so a payload is checked for size and JSON depth and not for shape.
//!   Bounded, not blessed.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use super::limits::{json_depth, InputLimits};
use super::request::ConversationRef;

/// Upper bound on a business payload, encoded.
///
/// Generous next to a metadata value (512 bytes) because a business event
/// legitimately carries a small object, and tight next to a request body because
/// this stream is meant for envelopes: anything large enough to need streaming
/// belongs behind a reference, not inline (the same rule the item types follow).
pub const MAX_BUSINESS_PAYLOAD_BYTES: usize = 16 * 1024;

/// Upper bound on the business `kind` discriminator, matching the metadata key
/// limit — both are short labels chosen by the caller.
pub const MAX_BUSINESS_KIND_BYTES: usize = 64;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SessionRequestViolation {
    #[error("kind must not be empty")]
    EmptyKind,
    #[error("kind exceeds {max} bytes")]
    KindTooLong { max: usize },
    /// Control characters would let a `kind` corrupt log lines that quote it.
    #[error("kind must not contain control characters")]
    KindHasControlChars,
    #[error("payload exceeds {max} bytes")]
    PayloadTooLarge { max: usize },
    #[error("payload nests deeper than {max}")]
    PayloadTooDeep { max: usize },
}

/// `POST /v1/sessions`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSessionRequest {
    /// Conversation to bind to. A fresh one is created when absent.
    ///
    /// Accepting an existing conversation matters for callers who started with
    /// the official SDK and only later want multi-device delivery: without it,
    /// adding a session would mean abandoning the history already accumulated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation: Option<ConversationRef>,
}

/// `POST /v1/sessions/{id}/events`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppendBusinessEventRequest {
    /// Caller-defined category. Data, never a metric label.
    pub kind: String,
    #[serde(default)]
    pub payload: Value,
}

impl AppendBusinessEventRequest {
    pub fn validate(&self, limits: &InputLimits) -> Result<(), SessionRequestViolation> {
        if self.kind.trim().is_empty() {
            return Err(SessionRequestViolation::EmptyKind);
        }
        if self.kind.len() > MAX_BUSINESS_KIND_BYTES {
            return Err(SessionRequestViolation::KindTooLong {
                max: MAX_BUSINESS_KIND_BYTES,
            });
        }
        if self.kind.chars().any(char::is_control) {
            return Err(SessionRequestViolation::KindHasControlChars);
        }

        // Encoded length, not the raw body length: the bound has to describe what
        // gets stored, and the request framing is a different budget (SEC-7).
        let encoded = serde_json::to_vec(&self.payload).map_err(|_| {
            SessionRequestViolation::PayloadTooLarge {
                max: MAX_BUSINESS_PAYLOAD_BYTES,
            }
        })?;
        if encoded.len() > MAX_BUSINESS_PAYLOAD_BYTES {
            return Err(SessionRequestViolation::PayloadTooLarge {
                max: MAX_BUSINESS_PAYLOAD_BYTES,
            });
        }

        // Depth is bounded separately from size: a small document can still nest
        // deeply enough to blow the stack of anything that walks it recursively
        // (INV-52).
        let max_depth = limits.max_json_depth;
        if json_depth(&self.payload) > max_depth {
            return Err(SessionRequestViolation::PayloadTooDeep { max: max_depth });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(kind: &str, payload: Value) -> AppendBusinessEventRequest {
        AppendBusinessEventRequest {
            kind: kind.into(),
            payload,
        }
    }

    #[test]
    fn accepts_a_well_formed_business_event() {
        let req = body("file_uploaded", serde_json::json!({ "path": "/tmp/a" }));
        assert!(req.validate(&InputLimits::default()).is_ok());
    }

    #[test]
    fn payload_defaults_to_null_and_is_accepted() {
        let req: AppendBusinessEventRequest =
            serde_json::from_str(r#"{"kind":"ping"}"#).unwrap();
        assert_eq!(req.payload, Value::Null);
        assert!(req.validate(&InputLimits::default()).is_ok());
    }

    #[test]
    fn rejects_kinds_that_are_empty_oversized_or_able_to_corrupt_a_log_line() {
        let limits = InputLimits::default();
        assert_eq!(
            body("", Value::Null).validate(&limits),
            Err(SessionRequestViolation::EmptyKind)
        );
        assert_eq!(
            body("   ", Value::Null).validate(&limits),
            Err(SessionRequestViolation::EmptyKind)
        );
        assert_eq!(
            body(&"k".repeat(MAX_BUSINESS_KIND_BYTES + 1), Value::Null).validate(&limits),
            Err(SessionRequestViolation::KindTooLong {
                max: MAX_BUSINESS_KIND_BYTES
            })
        );
        assert_eq!(
            body("a\nb", Value::Null).validate(&limits),
            Err(SessionRequestViolation::KindHasControlChars)
        );
    }

    #[test]
    fn bounds_payload_size_and_depth_independently() {
        let limits = InputLimits::default();

        // Large but shallow.
        let big = body(
            "k",
            serde_json::json!({ "blob": "x".repeat(MAX_BUSINESS_PAYLOAD_BYTES) }),
        );
        assert_eq!(
            big.validate(&limits),
            Err(SessionRequestViolation::PayloadTooLarge {
                max: MAX_BUSINESS_PAYLOAD_BYTES
            })
        );

        // Small but deep: caught by the depth rule, which the size rule would
        // have let straight through.
        let mut deep = Value::Null;
        for _ in 0..(limits.max_json_depth + 2) {
            deep = Value::Array(vec![deep]);
        }
        let deep = body("k", deep);
        assert_eq!(
            deep.validate(&limits),
            Err(SessionRequestViolation::PayloadTooDeep {
                max: limits.max_json_depth
            })
        );
    }

    #[test]
    fn envelopes_reject_unknown_fields() {
        assert!(serde_json::from_str::<AppendBusinessEventRequest>(
            r#"{"kind":"k","payload":{},"seq":3}"#
        )
        .is_err());
        assert!(serde_json::from_str::<CreateSessionRequest>(r#"{"session_id":"x"}"#).is_err());
    }

    #[test]
    fn a_session_may_bind_an_existing_conversation_in_either_upstream_form() {
        let req: CreateSessionRequest =
            serde_json::from_str(r#"{"conversation":"conv_x"}"#).unwrap();
        assert_eq!(req.conversation.as_ref().map(ConversationRef::id), Some("conv_x"));

        let req: CreateSessionRequest =
            serde_json::from_str(r#"{"conversation":{"id":"conv_y"}}"#).unwrap();
        assert_eq!(req.conversation.as_ref().map(ConversationRef::id), Some("conv_y"));

        let req: CreateSessionRequest = serde_json::from_str("{}").unwrap();
        assert_eq!(req.conversation, None);
    }
}
