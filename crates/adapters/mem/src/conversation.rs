//! Conversation store over the shared in-memory state (D27).
//!
//! Scope: **verification only** (L0–L2). The sql adapter is the production
//! carrier; this exists so the same port contract can be asserted without a
//! database (D17).

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use nova_responses_core::{
    Conversation, ConversationError, ConversationEvent, ConversationEventKind, ConversationId,
    ConversationStore, ResponseId, ResponseStatus, TenantId,
};

use crate::store::MemStore;

pub struct MemConversationStore {
    store: Arc<MemStore>,
}

impl MemConversationStore {
    pub fn new(store: Arc<MemStore>) -> Self {
        Self { store }
    }

    fn guard_available(&self) -> Result<(), ConversationError> {
        if self.store.is_unavailable() {
            // Callers must refuse the write, never proceed unstored (INV-46).
            return Err(ConversationError::Unavailable);
        }
        Ok(())
    }

    fn guard_writable(&self) -> Result<(), ConversationError> {
        self.guard_available()?;
        if self.store.is_read_only() {
            return Err(ConversationError::ReadOnly);
        }
        Ok(())
    }

    pub fn conversation_count(&self) -> usize {
        self.store.lock().conversations.len()
    }
}

#[async_trait]
impl ConversationStore for MemConversationStore {
    async fn create(
        &self,
        conversation: Conversation,
    ) -> Result<Conversation, ConversationError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        if g.conversations.len() >= self.store.max_conversations() {
            return Err(ConversationError::CapacityExceeded);
        }
        g.insert_conversation(conversation.clone());
        Ok(conversation)
    }

    async fn get(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<Option<Conversation>, ConversationError> {
        self.guard_available()?;
        let g = self.store.lock();
        Ok(match g.conversations.get(id) {
            None => None,
            // Tenant mismatch reads as absent, not forbidden (SEC-2).
            Some(c) if &c.tenant_id != tenant => None,
            Some(c) => Some(c.clone()),
        })
    }

    async fn update_metadata(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        metadata: BTreeMap<String, String>,
    ) -> Result<Conversation, ConversationError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        let Some(existing) = g.conversations.get(id) else {
            return Err(ConversationError::NotFound);
        };
        if &existing.tenant_id != tenant {
            return Err(ConversationError::NotFound);
        }
        let updated = Conversation {
            metadata,
            ..existing.clone()
        };
        g.insert_conversation(updated.clone());
        Ok(updated)
    }

    async fn delete(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<bool, ConversationError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        match g.conversations.get(id) {
            None => Ok(false),
            Some(c) if &c.tenant_id != tenant => Ok(false),
            Some(_) => Ok(g.remove_conversation(id).is_some()),
        }
    }

    async fn delete_by_tenant(&self, tenant: &TenantId) -> Result<u64, ConversationError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        let ids: Vec<ConversationId> = g
            .conversations_by_tenant
            .get(tenant)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default();
        let mut removed = 0u64;
        for id in ids {
            if g.remove_conversation(&id).is_some() {
                removed += 1;
            }
        }
        Ok(removed)
    }

    async fn advance(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        last: &ResponseId,
    ) -> Result<(), ConversationError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        let Some(existing) = g.conversations.get(id) else {
            return Err(ConversationError::NotFound);
        };
        if &existing.tenant_id != tenant {
            return Err(ConversationError::NotFound);
        }
        // Last write wins, on purpose: no compare-and-set, no conflict status
        // upstream never returns. See the port documentation.
        let updated = Conversation {
            last_response_id: Some(last.clone()),
            ..existing.clone()
        };
        g.insert_conversation(updated);
        Ok(())
    }

    async fn acquire_active(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        response_id: &ResponseId,
        now_ms: u64,
    ) -> Result<u64, ConversationError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        let Some(existing) = g.conversations.get(id) else {
            return Err(ConversationError::NotFound);
        };
        if &existing.tenant_id != tenant {
            return Err(ConversationError::NotFound);
        }
        match existing.active_response_id.clone() {
            None => {
                let updated = Conversation {
                    active_response_id: Some(response_id.clone()),
                    ..existing.clone()
                };
                g.insert_conversation(updated);
                let seq = g.push_conversation_event(
                    id,
                    ConversationEventKind::TurnStarted {
                        response_id: response_id.clone(),
                    },
                    now_ms,
                );
                if let Some(stream) = g.conversation_events.get_mut(id) {
                    stream.lock_seq = Some(seq);
                }
                Ok(seq)
            }
            Some(holder) if holder == *response_id => {
                // Re-entrant: return the sequence already assigned.
                Ok(g
                    .conversation_events
                    .get(id)
                    .and_then(|s| s.lock_seq)
                    .unwrap_or(0))
            }
            Some(holder) => Err(ConversationError::Busy { holder }),
        }
    }

    async fn release_active(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        response_id: &ResponseId,
        status: ResponseStatus,
        now_ms: u64,
    ) -> Result<u64, ConversationError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        let Some(existing) = g.conversations.get(id) else {
            return Ok(0);
        };
        if &existing.tenant_id != tenant {
            return Ok(0);
        }
        // Conditional release: only clear it if we still hold it.
        if existing.active_response_id.as_ref() != Some(response_id) {
            return Ok(0);
        }
        // Idempotent across execution-side retries.
        if let Some((last, seq)) = g
            .conversation_events
            .get(id)
            .and_then(|s| s.last_completed.clone())
        {
            if last == *response_id {
                return Ok(seq);
            }
        }
        let updated = Conversation {
            active_response_id: None,
            ..existing.clone()
        };
        g.insert_conversation(updated);
        let seq = g.push_conversation_event(
            id,
            ConversationEventKind::TurnCompleted {
                response_id: response_id.clone(),
                status,
            },
            now_ms,
        );
        if let Some(stream) = g.conversation_events.get_mut(id) {
            stream.last_completed = Some((response_id.clone(), seq));
        }
        Ok(seq)
    }

    async fn release_stale_active(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        holder: &ResponseId,
    ) -> Result<bool, ConversationError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        let Some(existing) = g.conversations.get(id) else {
            return Ok(false);
        };
        if &existing.tenant_id != tenant {
            return Ok(false);
        }
        if existing.active_response_id.as_ref() != Some(holder) {
            return Ok(false);
        }
        let updated = Conversation {
            active_response_id: None,
            ..existing.clone()
        };
        g.insert_conversation(updated);
        Ok(true)
    }

    async fn append_event(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        kind: ConversationEventKind,
        now_ms: u64,
    ) -> Result<u64, ConversationError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        let Some(existing) = g.conversations.get(id) else {
            return Err(ConversationError::NotFound);
        };
        if &existing.tenant_id != tenant {
            return Err(ConversationError::NotFound);
        }
        Ok(g.push_conversation_event(id, kind, now_ms))
    }

    async fn read_after(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        starting_after: Option<u64>,
        limit: usize,
        _wait_ms: u64,
    ) -> Result<Vec<ConversationEvent>, ConversationError> {
        self.guard_available()?;
        let g = self.store.lock();
        let Some(existing) = g.conversations.get(id) else {
            return Err(ConversationError::NotFound);
        };
        if &existing.tenant_id != tenant {
            return Err(ConversationError::NotFound);
        }
        let Some(stream) = g.conversation_events.get(id) else {
            return Ok(Vec::new());
        };
        let start = starting_after.map(|s| (s + 1) as usize).unwrap_or(0);
        Ok(stream
            .events
            .iter()
            .skip(start)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn list(&self, tenant: &TenantId) -> Result<Vec<Conversation>, ConversationError> {
        self.guard_available()?;
        let g = self.store.lock();
        let ids: Vec<ConversationId> = g
            .conversations_by_tenant
            .get(tenant)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default();
        let mut out: Vec<Conversation> = ids
            .iter()
            .filter_map(|id| g.conversations.get(id).cloned())
            .collect();
        // Newest first.
        out.sort_by_key(|c| std::cmp::Reverse(c.created_at_ms));
        Ok(out)
    }

    fn set_max_events_per_conversation(&self, _limit: usize) {
        // The in-memory carrier is bounded by `max_conversations`, not per-stream;
        // the sql adapter enforces the per-stream bound. No-op here.
    }

    async fn health(&self) -> Result<(), ConversationError> {
        self.guard_available()
    }
}
