use std::collections::HashMap;

use async_trait::async_trait;
use nova_core::{OutputEvent, TaskId};
use nova_ports::{StreamChannel, StreamError};
use parking_lot::Mutex;

#[derive(Default)]
struct Inner {
    /// task_id -> events (seq assigned on append)
    by_task: HashMap<TaskId, Vec<OutputEvent>>,
}

pub struct MemStreamChannel {
    inner: Mutex<Inner>,
}

impl MemStreamChannel {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
        }
    }
}

impl Default for MemStreamChannel {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StreamChannel for MemStreamChannel {
    async fn append(&self, mut event: OutputEvent) -> Result<u64, StreamError> {
        let mut g = self.inner.lock();
        let list = g.by_task.entry(event.task_id).or_default();
        let seq = list.len() as u64 + 1;
        event.seq = seq;
        list.push(event);
        Ok(seq)
    }

    async fn read_from(
        &self,
        task_id: TaskId,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<OutputEvent>, StreamError> {
        let g = self.inner.lock();
        let Some(list) = g.by_task.get(&task_id) else {
            return Ok(vec![]);
        };
        Ok(list
            .iter()
            .filter(|e| e.seq >= from_seq)
            .take(limit)
            .cloned()
            .collect())
    }
}
