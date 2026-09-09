//! Conversation store over the shared in-memory state (D27).
//!
//! Scope: **verification only** (L0–L2). The sql adapter is the production
//! carrier; this exists so the same port contract can be asserted without a
//! database (D17).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nova_responses::{
    Conversation, ConversationError, ConversationEvent, ConversationEventKind, ConversationId,
    ConversationStore, ResolvedContext, ResponseId, ResponseItem, ResponseStatus, StoreError,
    TenantId, TurnCommit,
};

use crate::store::{Inner, MemStore};

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
            return Err(ConversationError::Store(StoreError::Unavailable));
        }
        Ok(())
    }

    fn guard_writable(&self) -> Result<(), ConversationError> {
        self.guard_available()?;
        if self.store.is_read_only() {
            return Err(ConversationError::Store(StoreError::ReadOnly));
        }
        Ok(())
    }

    pub fn conversation_count(&self) -> usize {
        self.store.lock().conversations.len()
    }

    /// INV-59: refuse the append once the per-conversation event stream has
    /// reached its bound, rather than evicting the oldest event. Checked before
    /// any marker transition so a refused turn never leaves "occupied but no
    /// event" behind (INV-58).
    fn ensure_event_capacity(&self, g: &Inner, id: &ConversationId) -> Result<(), ConversationError> {
        let next = g
            .conversation_events
            .get(id)
            .map(|s| s.next_seq)
            .unwrap_or(0);
        if next as usize >= self.store.max_events_per_conversation() {
            return Err(ConversationError::CapacityExceeded);
        }
        Ok(())
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

    async fn read_snapshot(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<ResolvedContext, ConversationError> {
        self.guard_available()?;
        let g = self.store.lock();
        let Some(existing) = g.conversations.get(id) else {
            return Err(ConversationError::NotFound);
        };
        if &existing.tenant_id != tenant {
            return Err(ConversationError::NotFound);
        }
        let snap = g.snapshots.get(id);
        Ok(ResolvedContext {
            items: snap.map(|s| s.items.clone()).unwrap_or_default(),
            reasoning: snap.map(|s| s.reasoning.clone()).unwrap_or_default(),
            depth: snap.map(|s| s.turn_count).unwrap_or(0),
            bytes: snap.map(|s| s.bytes).unwrap_or(0),
        })
    }

    async fn append_turn(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        _response_id: &ResponseId,
        commit: TurnCommit,
        _now_ms: u64,
    ) -> Result<u64, ConversationError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        let Some(existing) = g.conversations.get(id) else {
            return Err(ConversationError::NotFound);
        };
        if &existing.tenant_id != tenant {
            return Err(ConversationError::NotFound);
        }
        let TurnCommit {
            input_items,
            output_items,
            reasoning,
            ..
        } = commit;
        let snap = g.snapshots.entry(id.clone()).or_default();
        let turn_start = snap.items.len();
        let input_len = input_items.len();
        snap.bytes += input_items.iter().map(ResponseItem::byte_len).sum::<usize>();
        snap.bytes += output_items.iter().map(ResponseItem::byte_len).sum::<usize>();
        snap.items.extend(input_items);
        snap.items.extend(output_items);
        snap.reasoning.resize(snap.items.len(), None);
        if let Some(text) = reasoning {
            // Reasoning precedes this turn's output, i.e. the item right after the
            // input block.
            let output_start = turn_start + input_len;
            if output_start < snap.reasoning.len() {
                snap.reasoning[output_start] = Some(text);
            }
        }
        let turn_index = snap.turn_count as u64;
        snap.turn_count += 1;
        Ok(turn_index)
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
                // INV-58: refuse before touching the marker, so a rejected turn
                // never leaves "occupied but no event" behind.
                self.ensure_event_capacity(&g, id)?;
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
                self.store.notify_conversation_event();
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
        self.ensure_event_capacity(&g, id)?;
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
        self.store.notify_conversation_event();
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
        self.ensure_event_capacity(&g, id)?;
        let seq = g.push_conversation_event(id, kind, now_ms);
        self.store.notify_conversation_event();
        Ok(seq)
    }

    async fn read_after(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        starting_after: Option<u64>,
        limit: usize,
        wait_ms: u64,
    ) -> Result<Vec<ConversationEvent>, ConversationError> {
        self.guard_available()?;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms);
        loop {
            {
                let g = self.store.lock();
                let Some(existing) = g.conversations.get(id) else {
                    return Err(ConversationError::NotFound);
                };
                if &existing.tenant_id != tenant {
                    return Err(ConversationError::NotFound);
                }
                let start = starting_after.map(|s| (s + 1) as usize).unwrap_or(0);
                // No stream yet means "created but nothing appended": wait for the
                // first event rather than immediately returning empty, or the SSE
                // skeleton would busy-loop on a freshly created conversation.
                if let Some(stream) = g.conversation_events.get(id) {
                    let batch: Vec<_> = stream
                        .events
                        .iter()
                        .skip(start)
                        .take(limit)
                        .cloned()
                        .collect();
                    if !batch.is_empty() {
                        return Ok(batch);
                    }
                }
            }
            // Long-poll deadline hit with nothing new: report empty and let the
            // caller (the SSE skeleton) decide whether to wait again.
            if tokio::time::Instant::now() >= deadline {
                return Ok(vec![]);
            }
            tokio::select! {
                _ = self.store.wait_conversation_event() => {}
                _ = tokio::time::sleep_until(deadline) => {}
            }
        }
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

    fn set_max_events_per_conversation(&self, limit: usize) {
        self.store.set_max_events_per_conversation(limit);
    }

    async fn health(&self) -> Result<(), ConversationError> {
        self.guard_available()
    }
}
