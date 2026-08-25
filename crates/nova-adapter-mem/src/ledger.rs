use std::collections::HashMap;

use async_trait::async_trait;
use nova_core::WorkerId;
use nova_ports::{CapacityLedger, LedgerError};
use parking_lot::Mutex;

#[derive(Default)]
struct WorkerCap {
    total: u32,
    used: u32,
}

pub struct MemCapacityLedger {
    inner: Mutex<HashMap<WorkerId, WorkerCap>>,
}

impl MemCapacityLedger {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for MemCapacityLedger {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CapacityLedger for MemCapacityLedger {
    async fn register_worker(&self, worker: WorkerId, total: u32) -> Result<(), LedgerError> {
        let mut g = self.inner.lock();
        g.entry(worker).or_insert(WorkerCap {
            total,
            used: 0,
        });
        if let Some(w) = g.get_mut(&worker) {
            w.total = total;
        }
        Ok(())
    }

    async fn remaining(&self, worker: &WorkerId) -> Result<u32, LedgerError> {
        let g = self.inner.lock();
        let w = g
            .get(worker)
            .ok_or_else(|| LedgerError::Internal("unknown worker".into()))?;
        Ok(w.total.saturating_sub(w.used))
    }

    async fn reserve(&self, worker: &WorkerId, units: u32) -> Result<(), LedgerError> {
        let mut g = self.inner.lock();
        let w = g
            .get_mut(worker)
            .ok_or_else(|| LedgerError::Internal("unknown worker".into()))?;
        if w.total.saturating_sub(w.used) < units {
            return Err(LedgerError::Insufficient);
        }
        w.used += units;
        Ok(())
    }

    async fn release(&self, worker: &WorkerId, units: u32) -> Result<(), LedgerError> {
        let mut g = self.inner.lock();
        let w = g
            .get_mut(worker)
            .ok_or_else(|| LedgerError::Internal("unknown worker".into()))?;
        w.used = w.used.saturating_sub(units);
        Ok(())
    }
}
