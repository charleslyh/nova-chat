//! Wire types for the conversation endpoints (D28).
//!
//! Four operations, matching upstream exactly: create, retrieve, update
//! metadata, delete. Note that update is a **POST to the same path**, not PATCH
//! or PUT — so there is no separate path type, only a separate body.
//!
//! The `items` sub-resource is absent. A conversation here is a pointer to the
//! tail of a response chain, so there are no items of its own to append, list,
//! retrieve or delete; history is read through the conversation's transcript,
//! which returns all of it in one call instead of obliging every caller to
//! paginate.
//!
//! The self-hosted business-event body also lives here (D28): the conversation
//! owns the event stream the former session layer held, so `append` is a
//! conversation sub-resource rather than a separate surface.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use super::limits::{json_depth, InputLimits};
use super::request::{validate_metadata, RequestViolation};

/// `POST /v1/conversations` and `POST /v1/conversations/{id}`.
///
/// Both endpoints accept only `metadata` (create seeds it, update replaces it
/// wholesale). A merge would need a way to spell "delete this key", which the
/// wire format does not have — so a merge would make deletion impossible rather
/// than merely verbose. One body type serves both so the two cannot drift into
/// disagreeing about what a valid metadata key is.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationMetadataRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<BTreeMap<String, String>>,
}

impl ConversationMetadataRequest {
    pub fn validate(&self) -> Result<(), RequestViolation> {
        if let Some(metadata) = &self.metadata {
            validate_metadata(metadata)?;
        }
        Ok(())
    }

    /// The metadata to store, defaulting to empty.
    pub fn metadata(&self) -> BTreeMap<String, String> {
        self.metadata.clone().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_is_optional_on_both_bodies() {
        let create: ConversationMetadataRequest = serde_json::from_str("{}").unwrap();
        assert!(create.validate().is_ok());
        assert!(create.metadata().is_empty());

        let update: ConversationMetadataRequest = serde_json::from_str("{}").unwrap();
        assert!(update.validate().is_ok());
        assert!(update.metadata().is_empty());
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
        let mut metadata = BTreeMap::new();
        for i in 0..=super::super::request::MAX_METADATA_ENTRIES {
            metadata.insert(format!("k{i}"), "v".into());
        }
        let req = ConversationMetadataRequest {
            metadata: Some(metadata),
        };
        assert!(matches!(
            req.validate(),
            Err(RequestViolation::TooManyMetadataEntries { .. })
        ));

        let long_key = "k".repeat(super::super::request::MAX_METADATA_KEY_BYTES + 1);
        let req = ConversationMetadataRequest {
            metadata: Some(BTreeMap::from([(long_key, "v".to_string())])),
        };
        assert!(matches!(
            req.validate(),
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
}

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

/// Validation failures for a business event body.
///
/// The payload stays opaque — the service cannot know the caller's domain, so a
/// payload is checked for size and JSON depth and not for shape. Bounded, not
/// blessed.
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
    pub fn validate(&self, limits: &InputLimits) -> Result<(), BusinessEventViolation> {
        if self.kind.trim().is_empty() {
            return Err(BusinessEventViolation::EmptyKind);
        }
        if self.kind.len() > MAX_BUSINESS_KIND_BYTES {
            return Err(BusinessEventViolation::KindTooLong {
                max: MAX_BUSINESS_KIND_BYTES,
            });
        }
        if self.kind.chars().any(char::is_control) {
            return Err(BusinessEventViolation::KindHasControlChars);
        }

        // Encoded length, not the raw body length: the bound has to describe what
        // gets stored, and the request framing is a different budget (SEC-7).
        let encoded = serde_json::to_vec(&self.payload).map_err(|_| {
            BusinessEventViolation::PayloadTooLarge {
                max: MAX_BUSINESS_PAYLOAD_BYTES,
            }
        })?;
        if encoded.len() > MAX_BUSINESS_PAYLOAD_BYTES {
            return Err(BusinessEventViolation::PayloadTooLarge {
                max: MAX_BUSINESS_PAYLOAD_BYTES,
            });
        }

        // Depth is bounded separately from size: a small document can still nest
        // deeply enough to blow the stack of anything that walks it recursively
        // (INV-52).
        let max_depth = limits.max_json_depth;
        if json_depth(&self.payload) > max_depth {
            return Err(BusinessEventViolation::PayloadTooDeep { max: max_depth });
        }
        Ok(())
    }
}

#[cfg(test)]
mod business_tests {
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
            Err(BusinessEventViolation::EmptyKind)
        );
        assert_eq!(
            body("   ", Value::Null).validate(&limits),
            Err(BusinessEventViolation::EmptyKind)
        );
        assert_eq!(
            body(&"k".repeat(MAX_BUSINESS_KIND_BYTES + 1), Value::Null).validate(&limits),
            Err(BusinessEventViolation::KindTooLong {
                max: MAX_BUSINESS_KIND_BYTES
            })
        );
        assert_eq!(
            body("a\nb", Value::Null).validate(&limits),
            Err(BusinessEventViolation::KindHasControlChars)
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
            Err(BusinessEventViolation::PayloadTooLarge {
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
