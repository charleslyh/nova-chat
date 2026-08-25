use std::collections::HashSet;

use async_trait::async_trait;
use nova_core::IdempotencyKey;
use nova_ports::{GateError, IdempotencyGate, Reservation};
use parking_lot::Mutex;

pub struct MemIdempotencyGate {
    keys: Mutex<HashSet<String>>,
}

impl MemIdempotencyGate {
    pub fn new() -> Self {
        Self {
            keys: Mutex::new(HashSet::new()),
        }
    }
}

impl Default for MemIdempotencyGate {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl IdempotencyGate for MemIdempotencyGate {
    async fn reserve(&self, key: &IdempotencyKey) -> Result<Reservation, GateError> {
        let mut g = self.keys.lock();
        if g.contains(&key.0) {
            return Ok(Reservation::AlreadyExists);
        }
        g.insert(key.0.clone());
        Ok(Reservation::Reserved)
    }
}
