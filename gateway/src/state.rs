//! Shared application state.
//!
//! Every port is held as a trait object: the ingress layer must not know which
//! backend is mounted. Storage is a shared carrier (`nova-responses-mem-server`),
//! so any node serves any request directly — there is no node-to-node forwarding and
//! therefore no topology flag here.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use nova_responses::{
    ContextStore, ConversationStore, MetricsSink, ResponseEventLog, ResponseLedger,
};

use crate::auth::KeyTable;
use nova_responses::config::Config;
use nova_responses::service::{ConversationsService, ResponsesService};

#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<Config>,
    pub ledger: Arc<dyn ResponseLedger>,
    pub event_log: Arc<dyn ResponseEventLog>,
    pub context: Arc<dyn ContextStore>,
    /// Held directly, not only behind [`ConversationsService`], because the SSE
    /// skeleton reads the stream itself: streaming is transport, and routing it
    /// through the capability layer would mean a service method whose only job is
    /// to hand a port back out.
    pub conversation_store: Arc<dyn ConversationStore>,
    pub now: Arc<dyn Fn() -> u64 + Send + Sync>,
    pub metrics: Arc<dyn MetricsSink>,
    pub keys: Arc<KeyTable>,
    pub service: Arc<ResponsesService>,
    pub conversations: Arc<ConversationsService>,
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

    pub fn now_ms(&self) -> u64 {
        (self.now)()
    }
}
