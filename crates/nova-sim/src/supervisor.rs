//! Process supervisor for simulated regions & mock workers.

use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RegionRole {
    Home,
    Edge,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionSpec {
    pub id: String,
    pub role: RegionRole,
    pub listen: String,
    #[serde(default)]
    pub home_upstream: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerView {
    pub id: String,
    pub region_id: String,
    pub capacity: u32,
    pub kind_bias: String,
    pub running: bool,
    pub home: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionView {
    pub id: String,
    pub role: RegionRole,
    pub listen: String,
    pub home_upstream: Option<String>,
    pub server_running: bool,
    pub healthy: bool,
    pub pending: Option<u64>,
    pub workers: Vec<WorkerView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    Home,
    Edge,
    Worker,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineEvent {
    /// Milliseconds since sim epoch.
    pub t_ms: u64,
    pub node_id: String,
    pub node_kind: NodeKind,
    pub action: String,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskView {
    pub id: String,
    pub kind: String,
    pub units: u32,
    pub state: String,
    pub attempt: u64,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub via_region: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineRow {
    pub node_id: String,
    pub node_kind: NodeKind,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimState {
    pub regions: Vec<RegionView>,
    pub tasks: Vec<TaskView>,
    pub timeline: Vec<TimelineEvent>,
    pub timeline_rows: Vec<TimelineRow>,
    pub events: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ListedTask {
    id: String,
    kind: String,
    units: u32,
    state: String,
    attempt: u64,
    owner: Option<String>,
}

struct WorkerProc {
    id: String,
    region_id: String,
    capacity: u32,
    kind_bias: String,
    home: String,
    child: Child,
}

struct RegionProc {
    spec: RegionSpec,
    config_path: PathBuf,
    child: Option<Child>,
}

#[derive(Debug, Clone)]
struct TrackedTask {
    via_region: Option<String>,
    kind: String,
    units: u32,
    state: String,
    attempt: u64,
    owner: Option<String>,
}

pub struct SimSupervisor {
    run_dir: PathBuf,
    regions: HashMap<String, RegionProc>,
    workers: HashMap<String, WorkerProc>,
    events: Vec<String>,
    timeline: Vec<TimelineEvent>,
    tracked: HashMap<String, TrackedTask>,
    epoch: Instant,
}

impl SimSupervisor {
    pub fn new() -> Result<Self> {
        let run_dir = PathBuf::from("target/nova-sim");
        fs::create_dir_all(&run_dir)?;
        fs::create_dir_all(run_dir.join("configs"))?;
        Ok(Self {
            run_dir,
            regions: HashMap::new(),
            workers: HashMap::new(),
            events: Vec::new(),
            timeline: Vec::new(),
            tracked: HashMap::new(),
            epoch: Instant::now(),
        })
    }

    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    fn log(&mut self, msg: impl Into<String>) {
        let m = msg.into();
        tracing::info!("{m}");
        self.events.push(m);
        if self.events.len() > 200 {
            let drain = self.events.len() - 200;
            self.events.drain(0..drain);
        }
    }

    fn push_tl(
        &mut self,
        node_id: impl Into<String>,
        node_kind: NodeKind,
        action: impl Into<String>,
        task_id: Option<String>,
        detail: impl Into<String>,
    ) {
        self.timeline.push(TimelineEvent {
            t_ms: self.now_ms(),
            node_id: node_id.into(),
            node_kind,
            action: action.into(),
            task_id,
            detail: detail.into(),
        });
        if self.timeline.len() > 500 {
            let drain = self.timeline.len() - 500;
            self.timeline.drain(0..drain);
        }
    }

    pub fn ensure_bins() -> Result<()> {
        for bin in ["nova-server", "nova-mock-worker"] {
            let st = Command::new("cargo").args(["build", "-p", bin]).status()?;
            if !st.success() {
                bail!("build {bin} failed");
            }
        }
        Ok(())
    }

    pub fn add_region(&mut self, spec: RegionSpec) -> Result<()> {
        if self.regions.contains_key(&spec.id) {
            bail!("region {} already exists", spec.id);
        }
        if matches!(spec.role, RegionRole::Edge) && spec.home_upstream.is_none() {
            bail!("edge region requires home_upstream");
        }
        for r in self.regions.values() {
            if r.spec.listen == spec.listen {
                bail!("listen {} already used by {}", spec.listen, r.spec.id);
            }
        }
        let config_path = self.write_region_config(&spec)?;
        let kind = match spec.role {
            RegionRole::Home => NodeKind::Home,
            RegionRole::Edge => NodeKind::Edge,
        };
        self.push_tl(
            spec.id.clone(),
            kind,
            "region_added",
            None,
            format!("{:?} @ {}", spec.role, spec.listen),
        );
        self.log(format!("region added: {} ({:?}) @ {}", spec.id, spec.role, spec.listen));
        self.regions.insert(
            spec.id.clone(),
            RegionProc {
                spec,
                config_path,
                child: None,
            },
        );
        Ok(())
    }

    fn write_region_config(&self, spec: &RegionSpec) -> Result<PathBuf> {
        let role = match spec.role {
            RegionRole::Home => "home",
            RegionRole::Edge => "edge",
        };
        let mut body = format!(
            "region = \"{}\"\nrole = \"{}\"\nlisten = \"{}\"\npending_threshold = 10000\n",
            spec.id, role, spec.listen
        );
        if let Some(up) = &spec.home_upstream {
            body.push_str(&format!("home_upstream = \"{up}\"\n"));
        }
        let path = self.run_dir.join("configs").join(format!("{}.toml", spec.id));
        fs::write(&path, body)?;
        Ok(path)
    }

    pub fn remove_region(&mut self, id: &str) -> Result<()> {
        self.stop_region(id)?;
        let wids: Vec<_> = self
            .workers
            .iter()
            .filter(|(_, w)| w.region_id == id)
            .map(|(k, _)| k.clone())
            .collect();
        for wid in wids {
            self.stop_worker(&wid)?;
        }
        if let Some(r) = self.regions.remove(id) {
            let kind = match r.spec.role {
                RegionRole::Home => NodeKind::Home,
                RegionRole::Edge => NodeKind::Edge,
            };
            self.push_tl(id, kind, "region_removed", None, "");
        }
        self.log(format!("region removed: {id}"));
        Ok(())
    }

    pub fn start_region(&mut self, id: &str) -> Result<()> {
        Self::ensure_bins()?;
        let region = self
            .regions
            .get_mut(id)
            .with_context(|| format!("unknown region {id}"))?;
        if let Some(child) = region.child.as_mut() {
            if child.try_wait().ok().flatten().is_none() {
                return Ok(());
            }
            region.child = None;
        }
        // Ensure listen port is free (orphans from prior sim sessions).
        if let Some(port) = parse_port(&region.spec.listen) {
            let _ = kill_listeners_on_port(port);
        }
        let role = region.spec.role.clone();
        let exe = PathBuf::from("target/debug/nova-server");
        let child = Command::new(&exe)
            .args(["--config", region.config_path.to_str().unwrap()])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("spawn nova-server")?;
        region.child = Some(child);
        let kind = match role {
            RegionRole::Home => NodeKind::Home,
            RegionRole::Edge => NodeKind::Edge,
        };
        self.push_tl(id, kind, "server_start", None, "");
        self.log(format!("region server started: {id}"));
        Ok(())
    }

    pub fn stop_region(&mut self, id: &str) -> Result<()> {
        if let Some(region) = self.regions.get_mut(id) {
            let role = region.spec.role.clone();
            if let Some(mut child) = region.child.take() {
                let _ = child.kill();
                let _ = child.wait();
                let kind = match role {
                    RegionRole::Home => NodeKind::Home,
                    RegionRole::Edge => NodeKind::Edge,
                };
                self.push_tl(id, kind, "server_stop", None, "");
                self.log(format!("region server stopped: {id}"));
            }
        }
        Ok(())
    }

    pub fn start_worker(
        &mut self,
        region_id: &str,
        capacity: u32,
        kind_bias: &str,
        work_ms: u64,
    ) -> Result<String> {
        Self::ensure_bins()?;
        let region = self
            .regions
            .get(region_id)
            .with_context(|| format!("unknown region {region_id}"))?;
        let home = match region.spec.role {
            RegionRole::Home => region.spec.listen.clone(),
            RegionRole::Edge => region
                .spec
                .home_upstream
                .clone()
                .context("edge missing home_upstream")?,
        };
        let id = Uuid::new_v4().to_string();
        let exe = PathBuf::from("target/debug/nova-mock-worker");
        let child = Command::new(&exe)
            .args([
                "--home",
                &home,
                "--capacity",
                &capacity.to_string(),
                "--kind-bias",
                kind_bias,
                "--worker-id",
                &id,
                "--region",
                region_id,
                "--work-ms",
                &work_ms.to_string(),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("spawn mock-worker")?;
        self.workers.insert(
            id.clone(),
            WorkerProc {
                id: id.clone(),
                region_id: region_id.to_string(),
                capacity,
                kind_bias: kind_bias.to_string(),
                home,
                child,
            },
        );
        self.push_tl(
            &id,
            NodeKind::Worker,
            "worker_start",
            None,
            format!("region={region_id} cap={capacity} bias={kind_bias}"),
        );
        self.log(format!(
            "worker started: {id} on region={region_id} capacity={capacity}"
        ));
        Ok(id)
    }

    pub fn stop_worker(&mut self, id: &str) -> Result<()> {
        if let Some(mut w) = self.workers.remove(id) {
            let _ = w.child.kill();
            let _ = w.child.wait();
            self.push_tl(id, NodeKind::Worker, "worker_stop", None, "");
            self.log(format!("worker stopped: {id}"));
        }
        Ok(())
    }

    pub fn stop_all(&mut self) -> Result<()> {
        let wids: Vec<_> = self.workers.keys().cloned().collect();
        for id in wids {
            self.stop_worker(&id)?;
        }
        let rids: Vec<_> = self.regions.keys().cloned().collect();
        for id in rids {
            self.stop_region(&id)?;
        }
        // Sweep common sim ports / binaries left by crashed sessions.
        for port in 18080..18090 {
            let _ = kill_listeners_on_port(port);
        }
        let _ = Command::new("pkill")
            .args(["-f", "target/debug/nova-mock-worker"])
            .status();
        self.log("all processes stopped");
        Ok(())
    }

    pub fn load_preset_three_region(&mut self) -> Result<()> {
        self.stop_all()?;
        self.regions.clear();
        self.tracked.clear();
        self.timeline.clear();
        self.epoch = Instant::now();
        self.add_region(RegionSpec {
            id: "home".into(),
            role: RegionRole::Home,
            listen: "127.0.0.1:18080".into(),
            home_upstream: None,
        })?;
        self.add_region(RegionSpec {
            id: "edge-b".into(),
            role: RegionRole::Edge,
            listen: "127.0.0.1:18081".into(),
            home_upstream: Some("127.0.0.1:18080".into()),
        })?;
        self.add_region(RegionSpec {
            id: "edge-c".into(),
            role: RegionRole::Edge,
            listen: "127.0.0.1:18082".into(),
            home_upstream: Some("127.0.0.1:18080".into()),
        })?;
        self.start_region("home")?;
        self.start_region("edge-b")?;
        self.start_region("edge-c")?;
        self.log("preset three-region loaded (1 home + 2 edge)");
        Ok(())
    }

    pub async fn wait_healthy(&self, id: &str, timeout: Duration) -> Result<()> {
        let listen = self
            .regions
            .get(id)
            .map(|r| r.spec.listen.clone())
            .context("unknown region")?;
        let start = Instant::now();
        loop {
            if probe_health(&listen).await.is_ok() {
                return Ok(());
            }
            if start.elapsed() > timeout {
                bail!("timeout waiting for {id} @ {listen}");
            }
            tokio::time::sleep(Duration::from_millis(80)).await;
        }
    }

    pub async fn submit_task(
        &mut self,
        region_id: &str,
        kind: &str,
        units: u32,
    ) -> Result<String> {
        let (listen, role) = {
            let r = self
                .regions
                .get(region_id)
                .with_context(|| format!("unknown region {region_id}"))?;
            (r.spec.listen.clone(), r.spec.role.clone())
        };
        let line = format!("SUBMIT {kind} {units}\n");
        let resp = rpc(&listen, &line).await?;
        let trimmed = resp.trim().to_string();
        if let Some(tid) = trimmed.strip_prefix("OK ") {
            let tid = tid.trim().to_string();
            match role {
                RegionRole::Edge => {
                    self.push_tl(
                        region_id,
                        NodeKind::Edge,
                        "submit_accept",
                        Some(tid.clone()),
                        format!("{kind} units={units}"),
                    );
                    self.push_tl(
                        "home",
                        NodeKind::Home,
                        "submit_commit",
                        Some(tid.clone()),
                        format!("via {region_id}"),
                    );
                }
                RegionRole::Home => {
                    self.push_tl(
                        region_id,
                        NodeKind::Home,
                        "submit_commit",
                        Some(tid.clone()),
                        format!("{kind} units={units}"),
                    );
                }
            }
            self.tracked.insert(
                tid.clone(),
                TrackedTask {
                    via_region: Some(region_id.to_string()),
                    kind: kind.to_string(),
                    units,
                    state: "pending".into(),
                    attempt: 0,
                    owner: None,
                },
            );
        }
        self.log(format!(
            "submit via {region_id}: kind={kind} units={units} → {trimmed}"
        ));
        Ok(resp)
    }

    fn home_listen(&self) -> Option<String> {
        self.regions
            .values()
            .find(|r| matches!(r.spec.role, RegionRole::Home))
            .map(|r| r.spec.listen.clone())
    }

    async fn sync_tasks_from_home(&mut self) {
        let Some(home) = self.home_listen() else {
            return;
        };
        let Ok(resp) = rpc(&home, "LIST\n").await else {
            return;
        };
        let Some(json) = resp.trim().strip_prefix("OK ") else {
            return;
        };
        let Ok(listed) = serde_json::from_str::<Vec<ListedTask>>(json) else {
            return;
        };
        for item in listed {
            let prev = self.tracked.get(&item.id).cloned();
            let via = prev.as_ref().and_then(|p| p.via_region.clone());
            if let Some(p) = &prev {
                if p.state != item.state {
                    let became_claimed = item.state == "claimed"
                        || ((item.state == "succeeded" || item.state == "failed")
                            && p.state == "pending");
                    if became_claimed {
                        if let Some(owner) = item.owner.as_ref().or(p.owner.as_ref()) {
                            self.push_tl(
                                owner,
                                NodeKind::Worker,
                                "claim",
                                Some(item.id.clone()),
                                format!("attempt={}", item.attempt),
                            );
                            self.push_tl(
                                "home",
                                NodeKind::Home,
                                "claim_grant",
                                Some(item.id.clone()),
                                format!("worker={}", &owner[..8.min(owner.len())]),
                            );
                        }
                    }
                    if item.state == "succeeded" || item.state == "failed" {
                        if let Some(owner) = item.owner.as_ref().or(p.owner.as_ref()) {
                            self.push_tl(
                                owner,
                                NodeKind::Worker,
                                "complete",
                                Some(item.id.clone()),
                                item.state.clone(),
                            );
                        }
                        self.push_tl(
                            "home",
                            NodeKind::Home,
                            "task_terminal",
                            Some(item.id.clone()),
                            item.state.clone(),
                        );
                    }
                }
            }
            self.tracked.insert(
                item.id.clone(),
                TrackedTask {
                    via_region: via,
                    kind: item.kind,
                    units: item.units,
                    state: item.state,
                    attempt: item.attempt,
                    owner: item.owner,
                },
            );
        }
    }

    pub async fn snapshot(&mut self) -> SimState {
        let mut dead_w = Vec::new();
        for (id, w) in &mut self.workers {
            if w.child.try_wait().ok().flatten().is_some() {
                dead_w.push(id.clone());
            }
        }
        for id in dead_w {
            self.workers.remove(&id);
            self.push_tl(&id, NodeKind::Worker, "worker_exit", None, "");
            self.log(format!("worker exited: {id}"));
        }
        for region in self.regions.values_mut() {
            if let Some(child) = &mut region.child {
                if child.try_wait().ok().flatten().is_some() {
                    region.child = None;
                }
            }
        }

        self.sync_tasks_from_home().await;

        let mut regions = Vec::new();
        let region_ids: Vec<_> = self.regions.keys().cloned().collect();
        for id in region_ids {
            let spec = self.regions[&id].spec.clone();
            let running = self.regions[&id].child.is_some();
            let (healthy, pending) = if running {
                match rpc(&spec.listen, "STATS\n").await {
                    Ok(resp) => {
                        let pending = resp
                            .split_whitespace()
                            .find_map(|t| t.strip_prefix("pending="))
                            .and_then(|s| s.parse().ok());
                        (resp.contains("OK"), pending)
                    }
                    Err(_) => (false, None),
                }
            } else {
                (false, None)
            };
            let workers: Vec<_> = self
                .workers
                .values()
                .filter(|w| w.region_id == id)
                .map(|w| WorkerView {
                    id: w.id.clone(),
                    region_id: w.region_id.clone(),
                    capacity: w.capacity,
                    kind_bias: w.kind_bias.clone(),
                    running: true,
                    home: w.home.clone(),
                })
                .collect();
            regions.push(RegionView {
                id: spec.id,
                role: spec.role,
                listen: spec.listen,
                home_upstream: spec.home_upstream,
                server_running: running,
                healthy,
                pending,
                workers,
            });
        }
        regions.sort_by(|a, b| a.id.cmp(&b.id));

        let mut tasks: Vec<_> = self
            .tracked
            .iter()
            .map(|(id, t)| TaskView {
                id: id.clone(),
                kind: t.kind.clone(),
                units: t.units,
                state: t.state.clone(),
                attempt: t.attempt,
                owner: t.owner.clone(),
                via_region: t.via_region.clone(),
            })
            .collect();
        tasks.sort_by(|a, b| a.id.cmp(&b.id));

        let mut timeline_rows = Vec::new();
        for r in &regions {
            timeline_rows.push(TimelineRow {
                node_id: r.id.clone(),
                node_kind: match r.role {
                    RegionRole::Home => NodeKind::Home,
                    RegionRole::Edge => NodeKind::Edge,
                },
                label: format!("{} ({})", r.id, match r.role {
                    RegionRole::Home => "home",
                    RegionRole::Edge => "edge",
                }),
            });
        }
        let mut wids: Vec<_> = self.workers.keys().cloned().collect();
        wids.sort();
        for id in wids {
            let w = &self.workers[&id];
            timeline_rows.push(TimelineRow {
                node_id: id.clone(),
                node_kind: NodeKind::Worker,
                label: format!("w:{}@{}", &id[..8.min(id.len())], w.region_id),
            });
        }

        SimState {
            regions,
            tasks,
            timeline: self.timeline.clone(),
            timeline_rows,
            events: self.events.clone(),
        }
    }
}

async fn probe_health(addr: &str) -> Result<()> {
    let resp = rpc(addr, "HEALTH\n").await?;
    if resp.contains("OK") {
        Ok(())
    } else {
        bail!("unhealthy: {resp}")
    }
}

async fn rpc(addr: &str, line: &str) -> Result<String> {
    let _: SocketAddr = addr.parse()?;
    let mut stream = TcpStream::connect(addr).await?;
    stream.write_all(line.as_bytes()).await?;
    let mut buf = vec![0u8; 65536];
    let n = stream.read(&mut buf).await?;
    Ok(std::str::from_utf8(&buf[..n])?.to_string())
}

pub type SharedSim = Arc<Mutex<SimSupervisor>>;

pub fn next_free_port_hint(used: &[u16], start: u16) -> u16 {
    let mut p = start;
    loop {
        if !used.contains(&p) {
            return p;
        }
        p += 1;
    }
}

pub fn parse_port(listen: &str) -> Option<u16> {
    listen.rsplit(':').next()?.parse().ok()
}

fn kill_listeners_on_port(port: u16) -> Result<()> {
    let out = Command::new("lsof")
        .args(["-nP", &format!("-iTCP:{port}"), "-sTCP:LISTEN", "-t"])
        .output();
    let Ok(out) = out else {
        return Ok(());
    };
    let text = String::from_utf8_lossy(&out.stdout);
    for pid in text.split_whitespace() {
        let _ = Command::new("kill").args(["-TERM", pid]).status();
    }
    std::thread::sleep(Duration::from_millis(50));
    Ok(())
}

#[allow(dead_code)]
fn _path_exists(p: &Path) -> bool {
    p.exists()
}
