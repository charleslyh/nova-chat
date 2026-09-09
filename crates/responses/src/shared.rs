//! Shared identifiers: tenant, node, agent, attempt, idempotency.
//!
//! These are the identity types that more than one domain concept uses. The
//! domain-owned ids (`ResponseId`, `ConversationId`) live with their domain —
//! see `context.rs` and `conversation.rs` — rather than being grouped here by
//! the technical role "identifier".

use std::fmt;
use std::str::FromStr;

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IdError {
    #[error("response id must start with `resp_`")]
    MissingPrefix,
    #[error("response id must have the form resp_<node>_<uuid>")]
    MalformedShape,
    #[error("node tag must be 1..={max} chars of [a-z0-9-]", max = NodeTag::MAX_LEN)]
    InvalidNodeTag,
    #[error("response id uuid part is not a uuid")]
    InvalidUuid,
    #[error("tenant id must be 1..={max} chars of [A-Za-z0-9._-]", max = TenantId::MAX_LEN)]
    InvalidTenantId,
    #[error("conversation id must have the form conv_<uuid>")]
    MalformedConversationId,
}

/// Node tag embedded in every response id so in-flight subscriptions can be
/// routed to the owning process (FR-30).
///
/// The character set is **deliberately restrictive**. This value is used as a
/// lookup key against the configured peer registry; permitting separators,
/// dots or slashes would open the door to constructing a tag that resembles a
/// host or path and thereby to route forgery / address injection (SEC-5).
/// Validation happens here, once, so no downstream component has to re-check.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeTag(String);

impl NodeTag {
    pub const MAX_LEN: usize = 32;

    pub fn parse(raw: &str) -> Result<Self, IdError> {
        if raw.is_empty() || raw.len() > Self::MAX_LEN {
            return Err(IdError::InvalidNodeTag);
        }
        let ok = raw
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
        if !ok {
            return Err(IdError::InvalidNodeTag);
        }
        Ok(Self(raw.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for NodeTag {
    type Err = IdError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Serialize for NodeTag {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for NodeTag {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        NodeTag::parse(&raw).map_err(de::Error::custom)
    }
}

/// Tenant identity resolved from the credential (SEC-2).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TenantId(String);

impl TenantId {
    pub const MAX_LEN: usize = 64;

    pub fn parse(raw: &str) -> Result<Self, IdError> {
        if raw.is_empty() || raw.len() > Self::MAX_LEN {
            return Err(IdError::InvalidTenantId);
        }
        let ok = raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
        if !ok {
            return Err(IdError::InvalidTenantId);
        }
        Ok(Self(raw.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for TenantId {
    type Err = IdError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Serialize for TenantId {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for TenantId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        TenantId::parse(&raw).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentId(pub Uuid);

impl AgentId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for AgentId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdempotencyKey(pub String);

/// Attempt is strictly monotonic and never resets (INV-5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct Attempt(pub u64);

impl Attempt {
    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_node_tags_that_could_forge_routing() {
        // Anything carrying a separator, case variation or non-ASCII must be
        // refused so a tag can never be shaped into a host, port or path.
        for bad in [
            "node_a",           // '_' would break id parsing
            "node.a",
            "node/a",
            "node:8080",
            "node\\a",
            "NODE",             // upper case
            "node a",
            "127.0.0.1",
            "",
            "评论",
            &"a".repeat(NodeTag::MAX_LEN + 1),
        ] {
            assert_eq!(
                NodeTag::parse(bad),
                Err(IdError::InvalidNodeTag),
                "node tag `{bad}` must be rejected"
            );
        }
    }

    #[test]
    fn safety_comes_from_registry_lookup_not_from_the_tag_text() {
        // `http` is a perfectly legal tag: it is only ever used as a key into
        // the configured peer registry, never concatenated into an address.
        // Rejecting suspicious-*looking* words would give a false sense of
        // security — the actual control is the whitelist lookup (SEC-5).
        assert!(NodeTag::parse("http").is_ok());
        assert!(NodeTag::parse("localhost").is_ok());
    }

    #[test]
    fn accepts_reasonable_node_tags() {
        for good in ["a", "node-a", "n1", "edge-b-2", &"a".repeat(NodeTag::MAX_LEN)] {
            assert!(NodeTag::parse(good).is_ok(), "`{good}` should be accepted");
        }
    }

    #[test]
    fn tenant_id_charset_is_bounded() {
        assert!(TenantId::parse("tenant-1").is_ok());
        assert!(TenantId::parse("acme.corp_1").is_ok());
        for bad in ["", "a b", "a'b", "a\"b", "a;b", "a/b", &"a".repeat(TenantId::MAX_LEN + 1)] {
            assert_eq!(
                TenantId::parse(bad),
                Err(IdError::InvalidTenantId),
                "tenant `{bad}` must be rejected"
            );
        }
    }

    #[test]
    fn attempt_is_monotonic() {
        let a = Attempt::default();
        assert_eq!(a.next(), Attempt(1));
        assert_eq!(a.next().next(), Attempt(2));
        assert!(a.next() > a);
    }
}
