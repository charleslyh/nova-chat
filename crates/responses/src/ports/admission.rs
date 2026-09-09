//! Node-local runtime controls.
//!
//! Separate from the storage ports on purpose. `read_only` and `pending_limit` are
//! process-local admission state that happens to be *checked* on the write path;
//! they are not persistence operations, they are synchronous, and the only consumer
//! is the admin surface. Bundling them into [`super::ResponseLedger`] obliged every
//! backend implementer to provide four methods that have nothing to do with a
//! ledger, and obliged the admin endpoints to depend on the whole ledger to flip a
//! boolean.
//!
//! It stays a supertrait of `ResponseLedger` because admission is decided at the
//! moment of a create, so whatever answers "may I write" must be the same object
//! that writes.

/// Degrade switches applied to this node only. Nodes are peers, so there is no
/// authority to broadcast from.
pub trait AdmissionControl: Send + Sync {
    /// INV-32: reject upstream writes while reads keep working.
    fn set_read_only(&self, enabled: bool);
    fn is_read_only(&self) -> bool;

    /// FR-33 overload threshold: queued/in-flight responses at or above this are
    /// refused rather than queued behind the running ones.
    fn set_pending_limit(&self, limit: usize);
    fn pending_limit(&self) -> usize;
}
