//! Shared application state.
//!
//! Every port is held as a trait object: the ingress layer must not know which backend
//! is mounted. Storage is a shared carrier, so any node serves any request directly —
//! there is no node-to-node forwarding and therefore no topology flag here.
//!
//! Two configs, because there are two layers: [`GatewayConfig`] is this layer's
//! (address, credentials variable, page size, drain budget) and reaches the capability
//! layer's own config through its `responses` field.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use nova_responses::config::Config;
use nova_responses::ports::{
    AdmissionControl, ConversationEvents, ConversationRepo, MetricsSink, ResponseEventLog,
    ResponseLedger,
};
use nova_responses::service::{ConversationsService, ResponsesService};
use nova_responses::Clock;

use crate::auth::KeyTable;
use crate::config::GatewayConfig;

#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<GatewayConfig>,
    pub ledger: Arc<dyn ResponseLedger>,
    pub event_log: Arc<dyn ResponseEventLog>,
    /// The conversation event stream, held directly because the SSE skeleton reads it
    /// itself: streaming is transport, and routing it through the capability layer would
    /// mean a service method whose only job is to hand a port back out.
    ///
    /// It is the *event* facet only — the ingress layer has no business deleting a
    /// conversation through this handle.
    pub conversation_events: Arc<dyn ConversationEvents>,
    /// The record facet, for the operational endpoints (health probe, tenant purge).
    pub conversation_repo: Arc<dyn ConversationRepo>,
    pub clock: Arc<dyn Clock>,
    pub metrics: Arc<dyn MetricsSink>,
    pub keys: Arc<KeyTable>,
    pub service: Arc<ResponsesService>,
    pub conversations: Arc<ConversationsService>,
    /// Cleared on SIGTERM so creation is refused while in-flight work drains (FR-34).
    /// Reads and subscriptions keep serving.
    pub accepting: Arc<AtomicBool>,
}

impl AppState {
    /// The capability layer's configuration, reached through this layer's.
    pub fn responses_cfg(&self) -> &Config {
        &self.cfg.responses
    }

    /// The node's degrade switches. A separate port from the ledger, so the admin
    /// surface depends on the four methods it flips and not on persistence.
    pub fn admission(&self) -> &dyn AdmissionControl {
        self.ledger.as_ref()
    }

    pub fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::SeqCst)
    }

    pub fn stop_accepting(&self) {
        self.accepting.store(false, Ordering::SeqCst);
    }

    pub fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }
}
