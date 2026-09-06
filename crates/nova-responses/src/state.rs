//! Shared application state.
//!
//! Every port is held as a trait object: the ingress layer must not know which
//! backend is mounted. Storage is a shared carrier (Postgres + Redis), so any
//! node serves any request directly — there is no node-to-node forwarding and
//! therefore no topology flag here.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use nova_responses_core::{
    Clock, ContextStore, ConversationStore, MetricsSink, ResponseEventLog, ResponseLedger,
    SessionStore,
};

use crate::auth::KeyTable;
use crate::config::Config;
use crate::service::{ConversationsService, ResponsesService, SessionsService};

#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<Config>,
    pub ledger: Arc<dyn ResponseLedger>,
    pub event_log: Arc<dyn ResponseEventLog>,
    pub context: Arc<dyn ContextStore>,
    pub conversation_store: Arc<dyn ConversationStore>,
    /// Held directly, not only behind [`SessionsService`], because the SSE
    /// skeleton reads the stream itself: streaming is transport, and routing it
    /// through the capability layer would mean a service method whose only job is
    /// to hand a port back out.
    pub session_store: Arc<dyn SessionStore>,
    pub clock: Arc<dyn Clock>,
    pub metrics: Arc<dyn MetricsSink>,
    pub keys: Arc<KeyTable>,
    pub service: Arc<ResponsesService>,
    pub conversations: Arc<ConversationsService>,
    pub sessions: Arc<SessionsService>,
    /// Cleared on SIGTERM so creation is refused while in-flight work drains
    /// (FR-34). Reads and subscriptions keep serving.
    pub accepting: Arc<AtomicBool>,
}

impl AppState {
    pub fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::SeqCst)
    }

    pub fn stop_accepting(&self) {
        self.accepting.store(false, Ordering::SeqCst);
    }

    pub async fn now_ms(&self) -> u64 {
        self.clock.now_ms().await
    }
}
