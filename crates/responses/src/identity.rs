//! Identity values shared by more than one domain concept: tenant, node, agent,
//! attempt, idempotency key.
//!
//! Only genuinely cross-cutting identities live here. The identities *owned* by a
//! single concept stay with it — [`crate::ResponseId`] in `response.rs`,
//! [`crate::ConversationId`] in `conversation.rs` — because grouping them here by
//! the technical role "identifier" would put a type further from the invariants
//! that constrain it.

use std::fmt;
use std::str::FromStr;

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;
use uuid::Uuid;

/// Why a string is not a valid identifier of some kind.
///
/// One type rather than one per id, because there is exactly one thing to say
/// about a rejected identifier: *what* it was supposed to be, and what that looks
/// like. Both fields are supplied by the type doing the parsing, so the message
/// can never describe the wrong kind of id — the failure mode of a shared enum
/// whose variants carry hard-coded prose.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid {kind}: expected {expected}")]
pub struct IdError {
    /// What was being parsed, e.g. `"response id"`.
    pub kind: &'static str,
    /// The accepted shape, e.g. `"resp_<node>_<uuid>"`.
    pub expected: &'static str,
}

impl IdError {
    pub const fn new(kind: &'static str, expected: &'static str) -> Self {
        Self { kind, expected }
    }
}

/// Node tag embedded in every response id so in-flight subscriptions can be
/// routed to the owning process (FR-30).
///
/// The character set is **deliberately restrictive**. This value is used as a
/// lookup key against the configured peer registry; permitting separators, dots
/// or slashes would open the door to constructing a tag that resembles a host or
/// path and thereby to route forgery / address injection (SEC-5). Validation
/// happens here, once, so no downstream component has to re-check.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeTag(String);

impl NodeTag {
    pub const MAX_LEN: usize = 32;
    const KIND: &'static str = "node tag";
    const EXPECTED: &'static str = "1..=32 characters of [a-z0-9-]";

    pub fn parse(raw: &str) -> Result<Self, IdError> {
        let ok = !raw.is_empty()
            && raw.len() <= Self::MAX_LEN
            && raw
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
        if ok {
            Ok(Self(raw.to_string()))
        } else {
            Err(Self::err())
        }
    }

    pub(crate) const fn err() -> IdError {
        IdError::new(Self::KIND, Self::EXPECTED)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Generate the string-shaped trait impls for a validated newtype.
///
/// [`Self::parse`] stays the only validator: `FromStr`, `Deserialize` and the
/// `Display` round trip all route through it, so a stored value that no longer
/// satisfies the rule fails loudly on read instead of silently entering the
/// system through a second door.
macro_rules! string_id_traits {
    ($name:ident) => {
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = IdError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::parse(s)
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let raw = String::deserialize(d)?;
                Self::parse(&raw).map_err(de::Error::custom)
            }
        }
    };
}

string_id_traits!(NodeTag);

/// Tenant identity resolved from the credential (SEC-2).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TenantId(String);

impl TenantId {
    pub const MAX_LEN: usize = 64;

    pub fn parse(raw: &str) -> Result<Self, IdError> {
        let ok = !raw.is_empty()
            && raw.len() <= Self::MAX_LEN
            && raw
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
        if ok {
            Ok(Self(raw.to_string()))
        } else {
            Err(IdError::new(
                "tenant id",
                "1..=64 characters of [A-Za-z0-9._-]",
            ))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

string_id_traits!(TenantId);

/// Identity of one execution process.
///
/// The inner uuid is private: an agent id is only ever minted here or read back
/// from storage, so there is no legitimate reason to assemble one from parts, and
/// a public field would make `AgentId` interchangeable with any other uuid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentId(Uuid);

impl AgentId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub fn uuid(self) -> Uuid {
        self.0
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

/// Caller-supplied idempotency key. Bounded like the other identity types: it
/// becomes a storage lookup key, so an unbounded value would let a caller force
/// arbitrarily large keys into every backend index.
///
/// The inner string is **private**. It was public, and the service promptly took
/// the shortcut of building a key without validating it — which is exactly the
/// hole a validated newtype exists to close. Every construction path now goes
/// through [`Self::parse`] or [`Self::for_response`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub const MAX_LEN: usize = 128;

    pub fn parse(raw: &str) -> Result<Self, IdError> {
        let ok = !raw.is_empty()
            && raw.len() <= Self::MAX_LEN
            && raw.bytes().all(|b| (0x21..=0x7e).contains(&b));
        if ok {
            Ok(Self(raw.to_string()))
        } else {
            Err(IdError::new(
                "idempotency key",
                "1..=128 visible ASCII characters with no whitespace",
            ))
        }
    }

    /// The default key for a generation that supplied none: its own id.
    ///
    /// Infallible by construction — a response id is visible ASCII well inside
    /// the length bound — which is why this exists instead of the caller
    /// `parse`-ing a string it just formatted and having to handle an error that
    /// cannot happen.
    pub fn for_response(response_id: &crate::ResponseId) -> Self {
        Self(response_id.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

string_id_traits!(IdempotencyKey);

/// Fencing token for one response. Strictly monotonic, never reset (INV-5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct Attempt(pub u64);

impl Attempt {
    /// A response that has never been claimed. The first claim raises the fence
    /// to 1, so `UNCLAIMED` is never a value an execution attempt runs under.
    pub const UNCLAIMED: Self = Self(0);

    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    pub fn is_unclaimed(self) -> bool {
        self == Self::UNCLAIMED
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
            "node_a", // '_' would break response id parsing
            "node.a",
            "node/a",
            "node:8080",
            "node\\a",
            "NODE", // upper case
            "node a",
            "127.0.0.1",
            "",
            "评论",
            &"a".repeat(NodeTag::MAX_LEN + 1),
        ] {
            let err = NodeTag::parse(bad).expect_err("must be rejected");
            assert_eq!(err.kind, "node tag", "node tag `{bad}`");
        }
    }

    #[test]
    fn safety_comes_from_registry_lookup_not_from_the_tag_text() {
        // `http` is a perfectly legal tag: it is only ever used as a key into the
        // configured peer registry, never concatenated into an address. Rejecting
        // suspicious-*looking* words would give a false sense of security — the
        // actual control is the whitelist lookup (SEC-5).
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
        for bad in [
            "",
            "a b",
            "a'b",
            "a\"b",
            "a;b",
            "a/b",
            &"a".repeat(TenantId::MAX_LEN + 1),
        ] {
            let err = TenantId::parse(bad).expect_err("must be rejected");
            assert_eq!(err.kind, "tenant id", "tenant `{bad}`");
        }
    }

    #[test]
    fn rejection_messages_name_the_kind_they_rejected() {
        // The point of carrying `kind`: one error type cannot claim a bad tenant
        // id "must start with `resp_`", which is what a shared enum of
        // hard-coded messages used to do.
        assert!(TenantId::parse("a b").unwrap_err().to_string().contains("tenant id"));
        assert!(NodeTag::parse("A").unwrap_err().to_string().contains("node tag"));
    }

    #[test]
    fn idempotency_keys_are_bounded_and_validated_on_every_path() {
        assert!(IdempotencyKey::parse("abc-123").is_ok());
        for bad in ["", "a b", &"k".repeat(IdempotencyKey::MAX_LEN + 1)] {
            assert!(IdempotencyKey::parse(bad).is_err(), "`{bad}`");
        }
        // Deserialisation validates too, so a malformed stored key fails on read.
        assert!(serde_json::from_str::<IdempotencyKey>(r#""a b""#).is_err());
        let key = IdempotencyKey::parse("k1").unwrap();
        assert_eq!(serde_json::to_string(&key).unwrap(), r#""k1""#);
    }

    #[test]
    fn attempt_is_monotonic_and_starts_unclaimed() {
        let a = Attempt::default();
        assert!(a.is_unclaimed());
        assert_eq!(a.next(), Attempt(1));
        assert!(!a.next().is_unclaimed());
        assert_eq!(a.next().next(), Attempt(2));
        assert!(a.next() > a);
    }

    #[test]
    fn attempt_saturates_instead_of_wrapping() {
        let max = Attempt(u64::MAX);
        assert_eq!(max.next(), max);
    }
}
