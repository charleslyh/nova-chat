//! Context store over the shared in-memory state.
//!
//! Scope: **verification only** (L0–L2). No durability — production uses the sql
//! adapter. In the carrier process this is the shared context store every node
//! reaches through the client stubs.

use std::sync::Arc;

use async_trait::async_trait;
use nova_responses_core::{
    canonical_items, ChainLimits, ContentIntegrity, ContextError, ContextStore, ResolvedContext,
    ResponseId, ResponseItem, ResponseStatus, StoredResponse, TenantId, Usage,
};

use crate::store::MemStore;

pub struct MemContextStore {
    store: Arc<MemStore>,
    integrity: Option<Arc<dyn ContentIntegrity>>,
}

impl MemContextStore {
    pub fn new(store: Arc<MemStore>) -> Self {
        Self {
            store,
            integrity: None,
        }
    }

    pub fn with_integrity(store: Arc<MemStore>, integrity: Arc<dyn ContentIntegrity>) -> Self {
        Self {
            store,
            integrity: Some(integrity),
        }
    }

    fn guard_available(&self) -> Result<(), ContextError> {
        if self.store.is_unavailable() {
            // Callers must refuse the write, never proceed unstored (INV-46).
            return Err(ContextError::Unavailable);
        }
        Ok(())
    }

    fn guard_writable(&self) -> Result<(), ContextError> {
        self.guard_available()?;
        if self.store.is_read_only() {
            return Err(ContextError::ReadOnly);
        }
        Ok(())
    }

    fn sign(&self, record: &mut StoredResponse) -> Result<(), ContextError> {
        if let Some(integrity) = &self.integrity {
            let canonical = canonical_payload(record);
            let tag = integrity
                .sign(&canonical)
                .map_err(|e| ContextError::Internal(e.to_string()))?;
            record.integrity = Some(tag);
            record.integrity_alg = Some(integrity.alg().to_string());
        }
        Ok(())
    }

    fn verify(&self, record: &StoredResponse) -> Result<(), ContextError> {
        let Some(integrity) = &self.integrity else {
            return Ok(());
        };
        let Some(tag) = &record.integrity else {
            return Ok(());
        };
        integrity
            .verify(&canonical_payload(record), tag)
            .map_err(|_| ContextError::IntegrityMismatch)
    }

    /// Test hook: corrupt stored content without updating the tag, to prove
    /// tampering is detectable (CR-13).
    pub fn tamper_for_test(&self, response_id: &ResponseId, replacement: Vec<ResponseItem>) -> bool {
        let mut g = self.store.lock();
        match g.records.get_mut(response_id) {
            Some(rec) => {
                rec.output_items = replacement;
                true
            }
            None => false,
        }
    }

    pub fn record_count(&self) -> usize {
        self.store.lock().records.len()
    }
}

/// Signing input: the canonical encoding of both item lists.
///
/// Instructions are excluded deliberately — they are metadata for echo, not
/// content, and including them would make the tag depend on a field that never
/// participates in chain resolution (INV-49).
fn canonical_payload(record: &StoredResponse) -> String {
    format!(
        "{}|{}",
        canonical_items(&record.input_items),
        canonical_items(&record.output_items)
    )
}

#[async_trait]
impl ContextStore for MemContextStore {
    async fn put(&self, mut record: StoredResponse) -> Result<(), ContextError> {
        self.guard_writable()?;
        // Sign before taking the lock: signing is pure computation, and doing it
        // inside the critical section would hold the lock for no reason.
        self.sign(&mut record)?;
        let mut g = self.store.lock();
        if !g.records.contains_key(&record.response_id)
            && g.records.len() >= self.store.max_records()
        {
            // Refuse rather than evict an existing record, which would break a
            // chain silently (INV-43).
            return Err(ContextError::CapacityExceeded);
        }
        g.insert_record(record);
        Ok(())
    }

    async fn append_output(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
        items: Vec<ResponseItem>,
        usage: Usage,
        status: ResponseStatus,
        now_ms: u64,
    ) -> Result<(), ContextError> {
        self.guard_writable()?;
        let mut record = {
            let g = self.store.lock();
            let Some(rec) = g.records.get(response_id) else {
                return Err(ContextError::NotFound);
            };
            if &rec.tenant_id != tenant {
                return Err(ContextError::NotFound);
            }
            rec.clone()
        };
        // Items arrive from the execution side already normalised; they are
        // never reconstructed from the event stream (INV-48).
        record.output_items = items;
        record.usage = usage;
        record.status = status;
        record.completed_at_ms = Some(now_ms);
        self.sign(&mut record)?;
        self.store.lock().insert_record(record);
        Ok(())
    }

    async fn get(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
    ) -> Result<Option<StoredResponse>, ContextError> {
        self.guard_available()?;
        let record = {
            let g = self.store.lock();
            match g.records.get(response_id) {
                None => return Ok(None),
                // Tenant mismatch is reported as absent, not forbidden (SEC-2).
                Some(rec) if &rec.tenant_id != tenant => return Ok(None),
                Some(rec) => rec.clone(),
            }
        };
        self.verify(&record)?;
        Ok(Some(record))
    }

    async fn resolve_chain(
        &self,
        tenant: &TenantId,
        from: &ResponseId,
        limits: ChainLimits,
    ) -> Result<ResolvedContext, ContextError> {
        self.guard_available()?;

        // History is materialised (D24): one record read, not a walk. The lock is
        // still held for the whole read so the snapshot cannot change underneath.
        let g = self.store.lock();

        let Some(record) = g.records.get(from) else {
            // The anchor is a value the caller supplied. Reporting "broken" rather
            // than "absent" avoids confirming existence across tenants, but it is
            // the same fatal outcome — no silent single-turn fallback (INV-43).
            return Err(ContextError::ChainBroken(from.to_string()));
        };
        if &record.tenant_id != tenant {
            return Err(ContextError::ChainBroken(from.to_string()));
        }
        if !record.stored {
            return Err(ContextError::NotStored);
        }
        self.verify(record)?;

        // History is a flat materialised copy (D24): the ancestors' snapshot plus
        // this response's own items. No walk, no segmentation.
        let mut items = record.context.clone();
        items.extend(record.chain_items().cloned());

        let depth = record.context_depth.saturating_add(1);
        if depth > limits.max_depth {
            return Err(ContextError::ChainTooLong {
                limit: limits.max_depth,
            });
        }
        if items.len() > limits.max_items {
            return Err(ContextError::ChainTooLong {
                limit: limits.max_items,
            });
        }
        let bytes: usize = items.iter().map(ResponseItem::byte_len).sum();
        if bytes > limits.max_bytes {
            return Err(ContextError::ChainTooLarge {
                limit: limits.max_bytes,
            });
        }

        Ok(ResolvedContext {
            items,
            depth,
            bytes,
        })
    }

    async fn delete(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
    ) -> Result<bool, ContextError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        match g.records.get(response_id) {
            None => Ok(false),
            Some(rec) if &rec.tenant_id != tenant => Ok(false),
            Some(_) => {
                // Record-level deletion only (D24): descendants hold their own flat
                // copy of the history, so removing this response does not touch them.
                // "Remove from the conversation" does not mean "erase from every
                // snapshot that inherited it" — that would be erasure, not removal.
                g.remove_record(response_id);
                Ok(true)
            }
        }
    }

    async fn delete_by_tenant(&self, tenant: &TenantId) -> Result<u64, ContextError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        let ids: Vec<ResponseId> = g
            .by_tenant
            .get(tenant)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default();
        let mut removed = 0u64;
        for id in ids {
            if g.remove_record(&id).is_some() {
                removed += 1;
            }
        }
        Ok(removed)
    }

    async fn sweep_expired(&self, now_ms: u64, limit: usize) -> Result<u64, ContextError> {
        let mut g = self.store.lock();
        // The index is keyed by `(deadline, id)` and therefore ordered by
        // deadline: stop at the first entry that is not yet due. This touches
        // only the records actually being removed, never the whole table.
        let due: Vec<ResponseId> = g
            .expiry
            .iter()
            .take_while(|((deadline, _), _)| *deadline <= now_ms)
            .take(limit)
            .map(|((_, id), _)| id.clone())
            .collect();
        let mut removed = 0u64;
        for id in due {
            if g.remove_record(&id).is_some() {
                removed += 1;
            }
        }
        Ok(removed)
    }

    async fn health(&self) -> Result<(), ContextError> {
        self.guard_available()
    }
}
