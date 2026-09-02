//! Process supervisor for peer gateway nodes and mock agents.
//!
//! Nodes are equivalent: each can create, each runs its own sweeper, and there is
//! no authority node (D20). What used to be a home/edge topology is now a flat
//! peer set, and the only remaining directed hop is for in-flight events.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use uuid::Uuid;

/// Console-only credentials, passed via the environment because that is the only
/// channel the service accepts them from (SEC-4). Never for deployment.
const FIXTURE_ENV: &[(&str, &str)] = &[
    ("NOVA_INTERNAL_TOKEN", "sim-internal-token"),
    ("NOVA_INTEGRITY_KEY", "sim-integrity-key-0123456789"),
];


pub type SharedSim = Arc<Mutex<SimSupervisor>>;

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
pub struct AgentView {
    pub id: String,
    pub region_id: String,
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
    pub agents: Vec<AgentView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    Home,
    Edge,
    Agent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineEvent {
    pub t_ms: u64,
    pub node_id: String,
    pub node_kind: NodeKind,
    pub action: String,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionView {
    pub id: String,
    pub region_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimSnapshot {
    pub regions: Vec<RegionView>,
    pub sessions: Vec<SessionView>,
    pub timeline: Vec<TimelineEvent>,
    pub epoch_ms: u64,
}

struct RegionRuntime {
    spec: RegionSpec,
    child: Option<Child>,
    config_path: PathBuf,
}

struct AgentRuntime {
    id: String,
    region_id: String,
    home: String,
    child: Option<Child>,
}

pub struct SimSupervisor {
    root: PathBuf,
    run_dir: PathBuf,
    regions: HashMap<String, RegionRuntime>,
    agents: HashMap<String, AgentRuntime>,
    sessions: Vec<SessionView>,
    timeline: Vec<TimelineEvent>,
    epoch: Instant,
}

impl SimSupervisor {
    pub fn new() -> Result<Self> {
        let root = workspace_root()?;
        let run_dir = root.join("target/sim");
        fs::create_dir_all(&run_dir)?;
        Ok(Self {
            root,
            run_dir,
            regions: HashMap::new(),
            agents: HashMap::new(),
            sessions: Vec::new(),
            timeline: Vec::new(),
            epoch: Instant::now(),
        })
    }

    pub fn ensure_bins() -> Result<()> {
        for pkg in ["nova-responses-gateway", "mock-agent"] {
            let st = Command::new("cargo").args(["build", "-p", pkg]).status()?;
            if !st.success() {
                bail!("build {pkg} failed");
            }
        }
        Ok(())
    }

    fn push_tl(&mut self, node_id: &str, kind: NodeKind, action: &str, session_id: Option<String>, detail: &str) {
        self.timeline.push(TimelineEvent {
            t_ms: self.epoch.elapsed().as_millis() as u64,
            node_id: node_id.into(),
            node_kind: kind,
            action: action.into(),
            session_id,
            detail: detail.into(),
        });
    }

    pub fn load_preset_two_region(&mut self) -> Result<()> {
        self.stop_all();
        self.regions.clear();
        self.agents.clear();
        self.sessions.clear();
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
        self.start_region("home")?;
        self.start_region("edge-b")?;
        self.start_agent("home", 8)?;
        self.push_tl("sim", NodeKind::Home, "preset", None, "two-region");
        Ok(())
    }

    pub fn add_region(&mut self, spec: RegionSpec) -> Result<()> {
        if self.regions.contains_key(&spec.id) {
            bail!("region {} exists", spec.id);
        }
        let cfg = self.write_config(&spec)?;
        self.regions.insert(
            spec.id.clone(),
            RegionRuntime {
                spec,
                child: None,
                config_path: cfg,
            },
        );
        Ok(())
    }

    fn write_config(&self, spec: &RegionSpec) -> Result<PathBuf> {
        let path = self.run_dir.join(format!("{}.toml", spec.id));
        let role = match spec.role {
            RegionRole::Home => "home",
            RegionRole::Edge => "edge",
        };
        let mut toml = format!(
            "region = \"{}\"\nrole = \"{role}\"\nlisten = \"{}\"\nrun_reaper = {}\n",
            spec.id,
            spec.listen,
            matches!(spec.role, RegionRole::Home)
        );
        if let Some(up) = &spec.home_upstream {
            toml.push_str(&format!("home_upstream = \"{up}\"\n"));
        }
        fs::write(&path, toml)?;
        Ok(path)
    }

    pub fn remove_region(&mut self, id: &str) -> Result<()> {
        self.stop_region(id)?;
        self.regions.remove(id);
        Ok(())
    }

    pub fn start_region(&mut self, id: &str) -> Result<()> {
        let rt = self.regions.get_mut(id).context("unknown region")?;
        if rt.child.is_some() {
            return Ok(());
        }
        let exe = self.root.join("target/debug/nova-sessions-gateway");
        let mut cmd = Command::new(&exe);
    for (key, value) in FIXTURE_ENV {
        cmd.env(key, value);
    }
    let child = cmd
            .args(["--config", rt.config_path.to_str().unwrap()])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("spawn nova-sessions-gateway")?;
        let kind = match rt.spec.role {
            RegionRole::Home => NodeKind::Home,
            RegionRole::Edge => NodeKind::Edge,
        };
        let listen = rt.spec.listen.clone();
        rt.child = Some(child);
        self.push_tl(id, kind, "start_sessions", None, &listen);
        Ok(())
    }

    pub fn stop_region(&mut self, id: &str) -> Result<()> {
        // stop agents on region
        let aids: Vec<_> = self
            .agents
            .iter()
            .filter(|(_, a)| a.region_id == id)
            .map(|(k, _)| k.clone())
            .collect();
        for a in aids {
            let _ = self.stop_agent(&a);
        }
        if let Some(rt) = self.regions.get_mut(id) {
            if let Some(mut c) = rt.child.take() {
                let _ = c.kill();
                let _ = c.wait();
            }
            self.push_tl(id, NodeKind::Home, "stop_sessions", None, "");
        }
        Ok(())
    }

    pub fn start_agent(&mut self, region_id: &str, tokens: usize) -> Result<String> {
        let rt = self.regions.get(region_id).context("unknown region")?;
        let home = match rt.spec.role {
            RegionRole::Home => rt.spec.listen.clone(),
            RegionRole::Edge => rt
                .spec
                .home_upstream
                .clone()
                .unwrap_or_else(|| rt.spec.listen.clone()),
        };
        let aid = Uuid::new_v4().to_string();
        let exe = self.root.join("target/debug/mock-agent");
        let mut cmd = Command::new(&exe);
    for (key, value) in FIXTURE_ENV {
        cmd.env(key, value);
    }
    let child = cmd
            .args([
                "--home",
                &home,
                "--agent-id",
                &aid,
                "--region",
                region_id,
                "--tokens",
                &tokens.to_string(),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("spawn agent")?;
        self.agents.insert(
            aid.clone(),
            AgentRuntime {
                id: aid.clone(),
                region_id: region_id.into(),
                home,
                child: Some(child),
            },
        );
        self.push_tl(&aid, NodeKind::Agent, "start_agent", None, region_id);
        Ok(aid)
    }

    pub fn stop_agent(&mut self, id: &str) -> Result<()> {
        if let Some(mut a) = self.agents.remove(id) {
            if let Some(mut c) = a.child.take() {
                let _ = c.kill();
                let _ = c.wait();
            }
            self.push_tl(id, NodeKind::Agent, "stop_agent", None, "");
        }
        Ok(())
    }

    pub fn stop_all(&mut self) {
        let aids: Vec<_> = self.agents.keys().cloned().collect();
        for a in aids {
            let _ = self.stop_agent(&a);
        }
        let rids: Vec<_> = self.regions.keys().cloned().collect();
        for r in rids {
            let _ = self.stop_region(&r);
        }
    }

    pub async fn wait_healthy(&self, id: &str, timeout: Duration) -> Result<()> {
        let listen = self
            .regions
            .get(id)
            .map(|r| r.spec.listen.clone())
            .context("unknown region")?;
        let start = Instant::now();
        loop {
            if http_get(&format!("http://{listen}/health"))
                .await
                .map(|b| b.contains("ok"))
                .unwrap_or(false)
            {
                return Ok(());
            }
            if start.elapsed() > timeout {
                bail!("timeout waiting healthy {id}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn create_session(&mut self, region_id: Option<&str>) -> Result<String> {
        let (rid, listen) = self.pick_region(region_id)?;
        let body = http_post(&format!("http://{listen}/v1/sessions"), "{}").await?;
        let v: serde_json::Value = serde_json::from_str(&extract_json_body(&body))?;
        let sid = v["session_id"]
            .as_str()
            .or_else(|| v["session_id"]["0"].as_str())
            .map(|s| s.to_string())
            .or_else(|| {
                // SessionId serializes as uuid string in newtype — may be nested
                v.get("session_id").and_then(|x| {
                    if let Some(s) = x.as_str() {
                        Some(s.to_string())
                    } else {
                        serde_json::to_string(x).ok().map(|s| s.trim_matches('"').to_string())
                    }
                })
            })
            .context("no session_id")?;
        self.sessions.push(SessionView {
            id: sid.clone(),
            region_id: rid.clone(),
        });
        self.push_tl(&rid, NodeKind::Home, "create_session", Some(sid.clone()), "");
        Ok(sid)
    }

    pub async fn submit_turn(
        &mut self,
        session_id: &str,
        text: &str,
        region_id: Option<&str>,
    ) -> Result<String> {
        let (rid, listen) = if let Some(r) = region_id {
            let listen = self
                .regions
                .get(r)
                .map(|x| x.spec.listen.clone())
                .context("region")?;
            (r.to_string(), listen)
        } else {
            self.pick_region(
                self.sessions
                    .iter()
                    .find(|s| s.id == session_id)
                    .map(|s| s.region_id.as_str()),
            )?
        };
        let payload = serde_json::json!({
            "text": text,
            "idempotency_key": Uuid::new_v4().to_string(),
        });
        let body = http_post(
            &format!("http://{listen}/v1/sessions/{session_id}/turns"),
            &payload.to_string(),
        )
        .await?;
        let v: serde_json::Value = serde_json::from_str(&extract_json_body(&body))?;
        let tid = v
            .get("turn_id")
            .and_then(|x| {
                x.as_str()
                    .map(|s| s.to_string())
                    .or_else(|| Some(x.to_string().trim_matches('"').to_string()))
            })
            .context("no turn_id")?;
        self.push_tl(
            &rid,
            NodeKind::Home,
            "submit_turn",
            Some(session_id.into()),
            &tid,
        );
        Ok(tid)
    }

    fn pick_region(&self, region_id: Option<&str>) -> Result<(String, String)> {
        if let Some(id) = region_id {
            let r = self.regions.get(id).context("unknown region")?;
            return Ok((id.into(), r.spec.listen.clone()));
        }
        let home = self
            .regions
            .values()
            .find(|r| matches!(r.spec.role, RegionRole::Home))
            .context("no home region")?;
        Ok((home.spec.id.clone(), home.spec.listen.clone()))
    }

    pub async fn snapshot(&mut self) -> SimSnapshot {
        let mut regions = Vec::new();
        for rt in self.regions.values() {
            let healthy = http_get(&format!("http://{}/health", rt.spec.listen))
                .await
                .map(|b| b.contains("ok"))
                .unwrap_or(false);
            let agents: Vec<_> = self
                .agents
                .values()
                .filter(|a| a.region_id == rt.spec.id)
                .map(|a| AgentView {
                    id: a.id.clone(),
                    region_id: a.region_id.clone(),
                    running: a.child.is_some(),
                    home: a.home.clone(),
                })
                .collect();
            regions.push(RegionView {
                id: rt.spec.id.clone(),
                role: rt.spec.role.clone(),
                listen: rt.spec.listen.clone(),
                home_upstream: rt.spec.home_upstream.clone(),
                server_running: rt.child.is_some(),
                healthy,
                agents,
            });
        }
        SimSnapshot {
            regions,
            sessions: self.sessions.clone(),
            timeline: self.timeline.clone(),
            epoch_ms: self.epoch.elapsed().as_millis() as u64,
        }
    }
}

pub fn parse_port(listen: &str) -> Option<u16> {
    listen.split(':').next_back()?.parse().ok()
}

pub fn next_free_port_hint(used: &[u16], start: u16) -> u16 {
    let mut p = start;
    while used.contains(&p) {
        p += 1;
    }
    p
}

fn workspace_root() -> Result<PathBuf> {
    let mut dir = std::env::current_dir()?;
    for _ in 0..6 {
        if dir.join("Cargo.toml").exists() && dir.join("crates").exists() {
            return Ok(dir);
        }
        if !dir.pop() {
            break;
        }
    }
    bail!("workspace root not found")
}

fn extract_json_body(http: &str) -> String {
    if let Some(idx) = http.find("\r\n\r\n") {
        http[idx + 4..].to_string()
    } else if let Some(idx) = http.find("\n\n") {
        http[idx + 2..].to_string()
    } else {
        http.to_string()
    }
}

async fn http_get(url: &str) -> Result<String> {
    simple_http("GET", url, None).await
}

async fn http_post(url: &str, body: &str) -> Result<String> {
    simple_http("POST", url, Some(body)).await
}

async fn simple_http(method: &str, url: &str, body: Option<&str>) -> Result<String> {
    let rest = url.trim_start_matches("http://");
    let (addr, path) = rest
        .split_once('/')
        .map(|(a, p)| (a, format!("/{p}")))
        .unwrap_or((rest, "/".into()));
    let mut stream = TcpStream::connect(addr).await?;
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    if let Some(b) = body {
        req.push_str("Content-Type: application/json\r\n");
        req.push_str(&format!("Content-Length: {}\r\n\r\n{b}", b.len()));
    } else if method == "POST" {
        req.push_str("Content-Length: 0\r\n\r\n");
    } else {
        req.push_str("\r\n");
    }
    stream.write_all(req.as_bytes()).await?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;
    Ok(String::from_utf8_lossy(&buf).to_string())
}
