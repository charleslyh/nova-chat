//! HMAC-SHA256 integrity implementation.
//!
//! Lives in the domain crate on purpose: it is pure computation with no
//! carrier product behind it, and every adapter must use the *same* scheme.
//! Duplicating it per adapter would let them drift, which would surface as
//! spurious integrity failures after a backend switch.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::ports::{ContentIntegrity, IntegrityError};

type HmacSha256 = Hmac<Sha256>;

pub const ALG: &str = "hmac-sha256-v1";

/// Environment variable holding the signing key.
///
/// Config files may only reference this **name**, never the value (SEC-4).
pub const KEY_ENV: &str = "NOVA_INTEGRITY_KEY";

pub struct HmacSha256Integrity {
    key: Vec<u8>,
}

impl HmacSha256Integrity {
    /// Read the key from the environment.
    ///
    /// Returns an error rather than falling back to a default so the caller can
    /// **fail at startup** (INV-44) — silently signing with a well-known key
    /// would make verification worthless while appearing to work.
    pub fn from_env() -> Result<Self, IntegrityError> {
        Self::from_env_var(KEY_ENV)
    }

    pub fn from_env_var(name: &str) -> Result<Self, IntegrityError> {
        let raw = std::env::var(name).map_err(|_| {
            IntegrityError::KeyUnavailable(format!("environment variable {name} is not set"))
        })?;
        Self::from_key(raw.as_bytes())
    }

    pub fn from_key(key: &[u8]) -> Result<Self, IntegrityError> {
        if key.len() < 16 {
            return Err(IntegrityError::KeyUnavailable(
                "key must be at least 16 bytes".into(),
            ));
        }
        Ok(Self { key: key.to_vec() })
    }

    fn tag(&self, canonical: &str) -> Result<String, IntegrityError> {
        let mut mac = HmacSha256::new_from_slice(&self.key)
            .map_err(|e| IntegrityError::Internal(e.to_string()))?;
        mac.update(canonical.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }
}

impl ContentIntegrity for HmacSha256Integrity {
    fn sign(&self, canonical: &str) -> Result<String, IntegrityError> {
        self.tag(canonical)
    }

    fn verify(&self, canonical: &str, expected: &str) -> Result<(), IntegrityError> {
        let actual = self.tag(canonical)?;
        // Constant-time: a short-circuiting comparison would leak the expected
        // tag one byte at a time to anyone able to measure response latency.
        let equal: bool = actual.as_bytes().ct_eq(expected.as_bytes()).into();
        if equal {
            Ok(())
        } else {
            Err(IntegrityError::Mismatch)
        }
    }

    fn alg(&self) -> &'static str {
        ALG
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn integrity() -> HmacSha256Integrity {
        HmacSha256Integrity::from_key(b"0123456789abcdef").unwrap()
    }

    #[test]
    fn sign_then_verify_round_trips() {
        let i = integrity();
        let tag = i.sign("payload").unwrap();
        assert_eq!(tag.len(), 64, "sha256 hex is 64 chars");
        assert!(i.verify("payload", &tag).is_ok());
    }

    #[test]
    fn detects_tampering() {
        let i = integrity();
        let tag = i.sign("payload").unwrap();
        assert_eq!(i.verify("payl0ad", &tag), Err(IntegrityError::Mismatch));
        assert_eq!(i.verify("payload", "deadbeef"), Err(IntegrityError::Mismatch));
        assert_eq!(i.verify("payload", ""), Err(IntegrityError::Mismatch));
    }

    #[test]
    fn different_keys_produce_different_tags() {
        let a = HmacSha256Integrity::from_key(b"0123456789abcdef").unwrap();
        let b = HmacSha256Integrity::from_key(b"fedcba9876543210").unwrap();
        assert_ne!(a.sign("x").unwrap(), b.sign("x").unwrap());
        // A tag from one key must not validate under another.
        let tag = a.sign("x").unwrap();
        assert_eq!(b.verify("x", &tag), Err(IntegrityError::Mismatch));
    }

    #[test]
    fn rejects_weak_or_missing_key_instead_of_defaulting() {
        assert!(matches!(
            HmacSha256Integrity::from_key(b"short"),
            Err(IntegrityError::KeyUnavailable(_))
        ));
        assert!(matches!(
            HmacSha256Integrity::from_env_var("NOVA_DEFINITELY_UNSET_KEY_VAR"),
            Err(IntegrityError::KeyUnavailable(_))
        ));
    }

    #[test]
    fn alg_marker_is_recorded() {
        assert_eq!(integrity().alg(), "hmac-sha256-v1");
    }

    #[test]
    fn tag_is_stable_across_instances_with_same_key() {
        let a = integrity().sign("same").unwrap();
        let b = integrity().sign("same").unwrap();
        assert_eq!(a, b);
    }
}
