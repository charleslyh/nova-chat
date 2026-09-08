use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum IntegrityError {
    /// Signing key absent or unusable. Surfaced at startup so the process
    /// fails fast rather than storing unverifiable records (INV-44).
    #[error("integrity key unavailable: {0}")]
    KeyUnavailable(String),
    #[error("integrity mismatch")]
    Mismatch,
    #[error("internal: {0}")]
    Internal(String),
}

/// Tamper detection for stored content.
///
/// Scope note: with plaintext retained, dispute resolution reads the plaintext
/// directly — this is **not** a non-repudiation mechanism, it detects storage
/// layer tampering only (D20 ⑧).
///
/// The signing key must come from the environment only, never from a config
/// file (SEC-4).
pub trait ContentIntegrity: Send + Sync {
    /// Sign canonicalised content.
    fn sign(&self, canonical: &str) -> Result<String, IntegrityError>;

    /// Verify in constant time — a timing-variable comparison would leak the
    /// expected tag byte by byte.
    fn verify(&self, canonical: &str, tag: &str) -> Result<(), IntegrityError>;

    /// Algorithm marker persisted alongside the tag so the scheme can be
    /// rotated without ambiguity.
    fn alg(&self) -> &'static str;
}
