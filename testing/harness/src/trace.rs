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
    SessionCreated {
        session_id: Uuid,
        at_ms: u64,
    },
    TurnSubmitted {
        session_id: Uuid,
        turn_id: Uuid,
        key: String,
        outcome: String,
        at_ms: u64,
    },
    TurnClaimed {
        session_id: Uuid,
        turn_id: Uuid,
        agent_id: Uuid,
        attempt: u64,
        at_ms: u64,
    },
    StreamAppended {
        session_id: Uuid,
        turn_id: Option<Uuid>,
        attempt: Option<u64>,
        seq: u64,
        kind: String,
        at_ms: u64,
    },
    StreamRead {
        session_id: Uuid,
        from_seq: u64,
        count: usize,
        gap: bool,
        at_ms: u64,
    },
    StreamTrimmed {
        session_id: Uuid,
        new_earliest: u64,
        at_ms: u64,
    },
    StreamAppendRejected {
        session_id: Uuid,
        turn_id: Option<Uuid>,
        attempt: Option<u64>,
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
    TurnTerminal {
        turn_id: Uuid,
        status: String,
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

    /// Persist every event as one JSON line under `testing/reports/traces/<name>.jsonl`.
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
