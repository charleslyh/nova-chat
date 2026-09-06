//! Wire types for the conversation endpoints (D27).
//!
//! Four operations, matching upstream exactly: create, retrieve, update
//! metadata, delete. Note that update is a **POST to the same path**, not PATCH
//! or PUT — so there is no separate path type, only a separate body.
//!
//! The `items` sub-resource is absent. A conversation here is a pointer to the
//! tail of a response chain, so there are no items of its own to append, list,
//! retrieve or delete; history is read through the session layer's transcript,
//! which returns all of it in one call instead of obliging every caller to
//! paginate.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::request::{validate_metadata, RequestViolation};

/// `POST /v1/conversations`.
///
/// Upstream also accepts an initial `items` array here. It is not accepted:
/// there is nowhere to put items that is not a second copy of content the
/// response chain already owns. Callers seed a conversation by making the first
/// generation against it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateConversationRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<BTreeMap<String, String>>,
}

impl CreateConversationRequest {
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

/// `POST /v1/conversations/{id}`.
///
/// Replaces metadata wholesale rather than merging. A merge would need a way to
/// spell "delete this key", which the wire format does not have — so a merge
/// would make deletion impossible rather than merely verbose.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateConversationRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<BTreeMap<String, String>>,
}

impl UpdateConversationRequest {
    pub fn validate(&self) -> Result<(), RequestViolation> {
        if let Some(metadata) = &self.metadata {
            validate_metadata(metadata)?;
        }
        Ok(())
    }

    pub fn metadata(&self) -> BTreeMap<String, String> {
        self.metadata.clone().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_is_optional_on_both_bodies() {
        let create: CreateConversationRequest = serde_json::from_str("{}").unwrap();
        assert!(create.validate().is_ok());
        assert!(create.metadata().is_empty());

        let update: UpdateConversationRequest = serde_json::from_str("{}").unwrap();
        assert!(update.validate().is_ok());
        assert!(update.metadata().is_empty());
    }

    #[test]
    fn unknown_fields_are_rejected() {
        // INV-50: the compatibility layer never ignores what it does not know.
        assert!(serde_json::from_str::<CreateConversationRequest>(r#"{"items":[]}"#).is_err());
        assert!(serde_json::from_str::<UpdateConversationRequest>(r#"{"topic":"x"}"#).is_err());
    }

    #[test]
    fn metadata_limits_are_the_request_side_limits() {
        // Reusing `validate_metadata` is the point: these bodies cannot develop
        // their own opinion about what a valid key is.
        let mut metadata = BTreeMap::new();
        for i in 0..=super::super::request::MAX_METADATA_ENTRIES {
            metadata.insert(format!("k{i}"), "v".into());
        }
        let req = CreateConversationRequest {
            metadata: Some(metadata),
        };
        assert!(matches!(
            req.validate(),
            Err(RequestViolation::TooManyMetadataEntries { .. })
        ));

        let long_key = "k".repeat(super::super::request::MAX_METADATA_KEY_BYTES + 1);
        let req = UpdateConversationRequest {
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
        let req: CreateConversationRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.metadata().get("topic").map(String::as_str), Some("demo"));
        assert_eq!(serde_json::to_string(&req).unwrap(), json);
    }
}
