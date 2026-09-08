//! Append-only verification trace (memory + optional JSONL file).

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TraceEvent {
    ResponseCreated {
        response_id: String,
        key: String,
        outcome: String,
        store: bool,
        previous: Option<String>,
        at_ms: u64,
    },
    ResponseClaimed {
        response_id: String,
        agent_id: Uuid,
        attempt: u64,
        at_ms: u64,
    },
    EventAppended {
        response_id: String,
        attempt: Option<u64>,
        sequence_number: u64,
        kind: String,
        at_ms: u64,
    },
    EventRead {
        response_id: String,
        starting_after: Option<u64>,
        count: usize,
        expired: bool,
        at_ms: u64,
    },
    EventAppendRejected {
        response_id: String,
        attempt: Option<u64>,
        reason: String,
        at_ms: u64,
    },
    ResponseTerminal {
        response_id: String,
        status: String,
        at_ms: u64,
    },
    /// Chain resolution succeeded, with the shape of what it returned.
    ChainResolved {
        response_id: String,
        depth: usize,
        items: usize,
        bytes: usize,
        at_ms: u64,
    },
    /// Chain resolution refused. `reason` is the error code, never a payload.
    ChainRejected {
        response_id: String,
        reason: String,
        at_ms: u64,
    },
    ContentStored {
        response_id: String,
        stored: bool,
        at_ms: u64,
    },
    IntegrityChecked {
        response_id: String,
        ok: bool,
        at_ms: u64,
    },
    CapacityRejected {
        scope: String,
        detail: String,
        at_ms: u64,
    },
    OrphanReclaimed {
        node_tag: String,
        count: usize,
        at_ms: u64,
    },
    PartialUsageRecorded {
        response_id: String,
        attempt: u64,
        total_tokens: u64,
        at_ms: u64,
    },
    DrainStarted {
        node_tag: String,
        in_flight: usize,
        at_ms: u64,
    },
    ProtocolRejected {
        reason: String,
        at_ms: u64,
    },
    ApiCall {
        method: String,
        url: String,
        status_ok: bool,
        detail: String,
        at_ms: u64,
    },
    MockState {
        component: String,
        detail: String,
        at_ms: u64,
    },
    FaultInjected {
        kind: String,
        target: String,
        at_ms: u64,
    },
    Clock {
        now_ms: u64,
    },
}

#[derive(Debug)]
pub struct Trace {
    pub name: String,
    pub events: Vec<TraceEvent>,
    file: Option<Mutex<File>>,
    path: Option<PathBuf>,
}

impl Trace {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            events: Vec::new(),
            file: None,
            path: None,
        }
    }

    /// Persist every event as one JSON line under `verify/reports/traces/<name>.jsonl`.
    pub fn with_jsonl_file(name: impl Into<String>, dir: &Path) -> anyhow::Result<Self> {
        let name = name.into();
        std::fs::create_dir_all(dir)?;
        let path = dir.join(format!("{name}.jsonl"));
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)?;
        Ok(Self {
            name,
            events: Vec::new(),
            file: Some(Mutex::new(file)),
            path: Some(path),
        })
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    pub fn push(&mut self, e: TraceEvent) {
        if let Some(file) = &self.file {
            if let Ok(mut f) = file.lock() {
                if let Ok(line) = serde_json::to_string(&e) {
                    let _ = writeln!(f, "{line}");
                    let _ = f.flush();
                }
            }
        }
        self.events.push(e);
    }
}
