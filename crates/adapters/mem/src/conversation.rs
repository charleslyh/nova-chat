//! Conversation store over the shared in-memory state (D27).
//!
//! Scope: **verification only** (L0–L2). The sql adapter is the production
//! carrier; this exists so the same port contract can be asserted without a
//! database (D17).

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use nova_responses_core::{
    Conversation, ConversationError, ConversationId, ConversationStore, ResponseId, TenantId,
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

    async fn health(&self) -> Result<(), ConversationError> {
        self.guard_available()
    }
}
