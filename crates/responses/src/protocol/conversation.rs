//! Wire types for the conversation endpoints (D28).
//!
//! Four operations, matching upstream exactly: create, retrieve, update metadata,
//! delete. Note that update is a **POST to the same path**, not PATCH or PUT — so
//! there is no separate path type, only a separate body.
//!
//! The `items` sub-resource is absent. A conversation here is a pointer to the tail
//! of a response chain, so there are no items of its own to append, list, retrieve
//! or delete; history is read through the conversation's transcript, which returns
//! all of it in one call instead of obliging every caller to paginate.
//!
//! The self-hosted business-event body also lives here (D28): the conversation owns
//! the event stream the former session layer held, so `append` is a conversation
//! sub-resource rather than a separate surface.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use super::limits::ProtocolLimits;
use super::request::{validate_metadata, RequestViolation};

/// `POST /v1/conversations` and `POST /v1/conversations/{id}`.
///
/// Both endpoints accept only `metadata` (create seeds it, update replaces it
/// wholesale). A merge would need a way to spell "delete this key", which the wire
/// format does not have — so a merge would make deletion impossible rather than
/// merely verbose. One body type serves both so the two cannot drift into
/// disagreeing about what a valid metadata key is.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationMetadataRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<BTreeMap<String, String>>,
}

impl ConversationMetadataRequest {
    pub fn validate(&self, limits: &ProtocolLimits) -> Result<(), RequestViolation> {
        match &self.metadata {
            Some(metadata) => validate_metadata(limits, metadata),
            None => Ok(()),
        }
    }

    /// The metadata to store, defaulting to empty.
    pub fn metadata(&self) -> BTreeMap<String, String> {
        self.metadata.clone().unwrap_or_default()
    }
}

/// Validation failures for a business event body.
///
/// The payload stays opaque — the service cannot know the caller's domain, so it is
/// checked for size and JSON depth and not for shape. Bounded, not blessed.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum BusinessEventViolation {
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

/// `POST /v1/conversations/{id}/events` (D28).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppendBusinessEventRequest {
    /// Caller-defined category. Data, never a metric label.
    pub kind: String,
    #[serde(default)]
    pub payload: Value,
}

impl AppendBusinessEventRequest {
    pub fn validate(&self, limits: &ProtocolLimits) -> Result<(), BusinessEventViolation> {
        if self.kind.trim().is_empty() {
            return Err(BusinessEventViolation::EmptyKind);
        }
        if self.kind.len() > limits.max_business_kind_bytes {
            return Err(BusinessEventViolation::KindTooLong {
                max: limits.max_business_kind_bytes,
            });
        }
        if self.kind.chars().any(char::is_control) {
            return Err(BusinessEventViolation::KindHasControlChars);
        }

        // Encoded length, not the raw body length: the bound has to describe what
        // gets stored, and the request framing is a different budget (SEC-7).
        let encoded = serde_json::to_vec(&self.payload).map_err(|_| {
            BusinessEventViolation::PayloadTooLarge {
                max: limits.max_business_payload_bytes,
            }
        })?;
        if encoded.len() > limits.max_business_payload_bytes {
            return Err(BusinessEventViolation::PayloadTooLarge {
                max: limits.max_business_payload_bytes,
            });
        }

        // Depth is bounded separately from size: a small document can still nest
        // deeply enough to blow the stack of anything that walks it recursively
        // (INV-52).
        if super::limits::json_depth(&self.payload) > limits.max_json_depth {
            return Err(BusinessEventViolation::PayloadTooDeep {
                max: limits.max_json_depth,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> ProtocolLimits {
        ProtocolLimits::default()
    }

    #[test]
    fn metadata_is_optional_on_both_bodies() {
        let body: ConversationMetadataRequest = serde_json::from_str("{}").unwrap();
        assert!(body.validate(&limits()).is_ok());
        assert!(body.metadata().is_empty());
    }

    #[test]
    fn unknown_fields_are_rejected() {
        // INV-50: the compatibility layer never ignores what it does not know.
        assert!(serde_json::from_str::<ConversationMetadataRequest>(r#"{"items":[]}"#).is_err());
        assert!(serde_json::from_str::<ConversationMetadataRequest>(r#"{"topic":"x"}"#).is_err());
    }

    #[test]
    fn metadata_limits_are_the_request_side_limits() {
        // Reusing `validate_metadata` is the point: these bodies cannot develop
        // their own opinion about what a valid key is.
        let limits = limits();
        let mut metadata = BTreeMap::new();
        for i in 0..=limits.max_metadata_entries {
            metadata.insert(format!("k{i}"), "v".into());
        }
        let req = ConversationMetadataRequest {
            metadata: Some(metadata),
        };
        assert!(matches!(
            req.validate(&limits),
            Err(RequestViolation::TooManyMetadataEntries { .. })
        ));

        let long_key = "k".repeat(limits.max_metadata_key_bytes + 1);
        let req = ConversationMetadataRequest {
            metadata: Some(BTreeMap::from([(long_key, "v".to_string())])),
        };
        assert!(matches!(
            req.validate(&limits),
            Err(RequestViolation::MetadataKeyTooLong { .. })
        ));
    }

    #[test]
    fn accepted_metadata_round_trips() {
        let json = r#"{"metadata":{"topic":"demo"}}"#;
        let req: ConversationMetadataRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.metadata().get("topic").map(String::as_str), Some("demo"));
        assert_eq!(serde_json::to_string(&req).unwrap(), json);
    }

    fn body(kind: &str, payload: Value) -> AppendBusinessEventRequest {
        AppendBusinessEventRequest {
            kind: kind.into(),
            payload,
        }
    }

    #[test]
    fn accepts_a_well_formed_business_event() {
        let req = body("file_uploaded", serde_json::json!({ "path": "/tmp/a" }));
        assert!(req.validate(&limits()).is_ok());
    }

    #[test]
    fn payload_defaults_to_null_and_is_accepted() {
        let req: AppendBusinessEventRequest = serde_json::from_str(r#"{"kind":"ping"}"#).unwrap();
        assert_eq!(req.payload, Value::Null);
        assert!(req.validate(&limits()).is_ok());
    }

    #[test]
    fn rejects_kinds_that_are_empty_oversized_or_able_to_corrupt_a_log_line() {
        let limits = limits();
        assert_eq!(
            body("", Value::Null).validate(&limits),
            Err(BusinessEventViolation::EmptyKind)
        );
        assert_eq!(
            body("   ", Value::Null).validate(&limits),
            Err(BusinessEventViolation::EmptyKind)
        );
        assert_eq!(
            body(&"k".repeat(limits.max_business_kind_bytes + 1), Value::Null).validate(&limits),
            Err(BusinessEventViolation::KindTooLong {
                max: limits.max_business_kind_bytes
            })
        );
        assert_eq!(
            body("a\nb", Value::Null).validate(&limits),
            Err(BusinessEventViolation::KindHasControlChars)
        );
    }

    #[test]
    fn bounds_payload_size_and_depth_independently() {
        let limits = limits();

        // Large but shallow.
        let big = body(
            "k",
            serde_json::json!({ "blob": "x".repeat(limits.max_business_payload_bytes) }),
        );
        assert_eq!(
            big.validate(&limits),
            Err(BusinessEventViolation::PayloadTooLarge {
                max: limits.max_business_payload_bytes
            })
        );

        // Small but deep: caught by the depth rule, which the size rule would have
        // let straight through.
        let mut deep = Value::Null;
        for _ in 0..(limits.max_json_depth + 2) {
            deep = Value::Array(vec![deep]);
        }
        assert_eq!(
            body("k", deep).validate(&limits),
            Err(BusinessEventViolation::PayloadTooDeep {
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
    }
}
