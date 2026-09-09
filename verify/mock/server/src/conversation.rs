//! Conversation store over the shared in-memory state (D27/D30).
//!
//! Four impl blocks, one per port facet: records, snapshot, turn lock, event stream. The
//! composition [`ConversationStore`] comes free from its blanket impl, so assembly still
//! mounts one object while each consumer depends only on the facet it uses.
//!
//! Scope: **verification only** (L0–L2). The sql adapter is the production carrier; this
//! exists so the same port contract can be asserted without a database (D17).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nova_responses::ports::{
    ConversationError, ConversationEvents, ConversationRepo, ConversationSnapshots, StoreError,
    TurnLock,
};
use nova_responses::{
    ContextEntry, Conversation, ConversationEvent, ConversationEventKind, ConversationId,
    ResolvedContext, ResponseId, ResponseStatus, TenantId, TurnCommit,
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

    /// The conversation, if it exists and belongs to `tenant`. Tenant mismatch reads as
    /// absent, never as forbidden (SEC-2).
    fn owned<'a>(
        g: &'a Inner,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Option<&'a Conversation> {
        g.conversations
            .get(id)
            .filter(|c| &c.tenant_id == tenant)
    }

    /// INV-59: refuse the append once the per-conversation event stream has reached its
    /// bound, rather than evicting the oldest event. Checked before any marker transition
    /// so a refused turn never leaves "occupied but no event" behind (INV-58).
    fn ensure_event_capacity(
        &self,
        g: &Inner,
        id: &ConversationId,
    ) -> Result<(), ConversationError> {
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
impl ConversationRepo for MemConversationStore {
    async fn create(&self, conversation: Conversation) -> Result<Conversation, ConversationError> {
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
        Ok(Self::owned(&g, tenant, id).cloned())
    }

    async fn update_metadata(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        metadata: BTreeMap<String, String>,
    ) -> Result<Conversation, ConversationError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        let existing = Self::owned(&g, tenant, id).ok_or(ConversationError::NotFound)?;
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
        if Self::owned(&g, tenant, id).is_none() {
            return Ok(false);
        }
        Ok(g.remove_conversation(id).is_some())
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

    async fn health(&self) -> Result<(), ConversationError> {
        self.guard_available()
    }
}

#[async_trait]
impl ConversationSnapshots for MemConversationStore {
    async fn read_snapshot(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<ResolvedContext, ConversationError> {
        self.guard_available()?;
        let g = self.store.lock();
        Self::owned(&g, tenant, id).ok_or(ConversationError::NotFound)?;
        let snap = g.snapshots.get(id);
        Ok(ResolvedContext::new(
            snap.map(|s| s.entries.clone()).unwrap_or_default(),
            snap.map(|s| s.turn_count).unwrap_or(0),
        ))
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
        Self::owned(&g, tenant, id).ok_or(ConversationError::NotFound)?;
        let TurnCommit {
            input_items,
            output_items,
            reasoning,
            ..
        } = commit;
        let snap = g.snapshots.entry(id.clone()).or_default();
        snap.entries
            .extend(input_items.into_iter().map(ContextEntry::new));
        // Reasoning precedes this turn's output, i.e. it belongs to the first output item.
        // Attaching it to that entry is the whole reason entries carry it: there is no
        // index arithmetic to get wrong and no parallel vector to keep aligned.
        let mut reasoning = reasoning;
        for item in output_items {
            snap.entries
                .push(ContextEntry::with_reasoning(item, reasoning.take()));
        }
        let turn_index = snap.turn_count as u64;
        snap.turn_count += 1;
        Ok(turn_index)
    }

    async fn advance(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        last: &ResponseId,
    ) -> Result<(), ConversationError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        let existing = Self::owned(&g, tenant, id).ok_or(ConversationError::NotFound)?;
        // Last write wins, on purpose: no compare-and-set, no conflict status upstream
        // never returns. See the port documentation.
        let updated = Conversation {
            last_response_id: Some(last.clone()),
            ..existing.clone()
        };
        g.insert_conversation(updated);
        Ok(())
    }
}

#[async_trait]
impl TurnLock for MemConversationStore {
    async fn acquire_active(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        response_id: &ResponseId,
        now_ms: u64,
    ) -> Result<u64, ConversationError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        let existing = Self::owned(&g, tenant, id).ok_or(ConversationError::NotFound)?;
        match existing.active_response_id.clone() {
            None => {
                // INV-58: refuse before touching the marker, so a rejected turn never
                // leaves "occupied but no event" behind.
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
                Ok(g.conversation_events
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
        let Some(existing) = Self::owned(&g, tenant, id) else {
            return Ok(0);
        };
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
        let Some(existing) = Self::owned(&g, tenant, id) else {
            return Ok(false);
        };
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
}

#[async_trait]
impl ConversationEvents for MemConversationStore {
    async fn append_event(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        kind: ConversationEventKind,
        now_ms: u64,
    ) -> Result<u64, ConversationError> {
        self.guard_writable()?;
        let mut g = self.store.lock();
        Self::owned(&g, tenant, id).ok_or(ConversationError::NotFound)?;
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
        wait: Duration,
    ) -> Result<Vec<ConversationEvent>, ConversationError> {
        self.guard_available()?;
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            {
                let g = self.store.lock();
                Self::owned(&g, tenant, id).ok_or(ConversationError::NotFound)?;
                let start = starting_after.map(|s| (s + 1) as usize).unwrap_or(0);
                // No stream yet means "created but nothing appended": wait for the first
                // event rather than immediately returning empty, or the SSE skeleton
                // would busy-loop on a freshly created conversation.
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
            // Long-poll deadline hit with nothing new: report empty and let the caller
            // (the SSE skeleton) decide whether to wait again.
            if tokio::time::Instant::now() >= deadline {
                return Ok(vec![]);
            }
            tokio::select! {
                _ = self.store.wait_conversation_event() => {}
                _ = tokio::time::sleep_until(deadline) => {}
            }
        }
    }

    fn set_max_events(&self, limit: usize) {
        self.store.set_max_events_per_conversation(limit);
    }
}
