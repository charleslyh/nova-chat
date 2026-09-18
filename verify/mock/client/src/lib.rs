//! IPC client adapters (data plane) for the in-memory shared carrier.
//!
//! These implement the same ports as the in-process [`mock_server`] types, but
//! each operation is a `POST /rpc` round-trip to `nova-responses-mem-server`.
//! The carrier owns the shared data; the client owns the **per-node** storage
//! degrade switch (`read_only`) as a process-local atomic.

mod conversation;
mod event_log;
mod ledger;
mod rpc;

pub use conversation::MemConversationClient;
pub use event_log::MemEventLogClient;
pub use ledger::MemLedgerClient;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use rpc::Rpc;

/// The assembled client side of the shared carrier: five adapters over one
/// logical node, sharing its local runtime controls.
pub struct MemClientWorld {
    pub ledger: Arc<MemLedgerClient>,
    pub event_log: Arc<MemEventLogClient>,
    pub conversation: Arc<MemConversationClient>,
}

impl MemClientWorld {
    /// Connect to the carrier's data plane at `base_url` (e.g. `http://127.0.0.1:19000`).
    pub fn new(base_url: &str) -> Self {
        let rpc = Arc::new(Rpc::new(base_url));
        // Shared *per-node* control: flipping read_only on the ledger is visible
        // to the event log of the same node.
        let read_only = Arc::new(AtomicBool::new(false));

        let ledger = Arc::new(MemLedgerClient::new(rpc.clone(), read_only.clone()));
        let event_log = Arc::new(MemEventLogClient::new(rpc.clone(), read_only.clone()));
        let conversation = Arc::new(MemConversationClient::new(
            rpc.clone(),
            read_only.clone(),
        ));

        Self {
            ledger,
            event_log,
            conversation,
        }
    }
}
