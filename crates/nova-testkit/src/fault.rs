use std::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultKind {
    KillWorker,
    Partition,
    Slow,
}

pub struct FaultInjector {
    injected: Mutex<Vec<(FaultKind, String)>>,
}

impl FaultInjector {
    pub fn new() -> Self {
        Self {
            injected: Mutex::new(Vec::new()),
        }
    }

    pub fn inject(&self, kind: FaultKind, target: impl Into<String>) {
        self.injected.lock().unwrap().push((kind, target.into()));
    }

    pub fn has(&self, kind: FaultKind) -> bool {
        self.injected.lock().unwrap().iter().any(|(k, _)| *k == kind)
    }
}

impl Default for FaultInjector {
    fn default() -> Self {
        Self::new()
    }
}
