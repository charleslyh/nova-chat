//! Thin composition root. Role via config (home vs edge), not separate crates.
//! Edge SUBMIT forwards to `home_upstream` (closer to real multi-region ingress).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use nova_adapter_mem::MemWorld;
use nova_claim::ClaimService;
use nova_core::{
    Attempt, CapacityNeed, IdempotencyKey, Priority, TaskId, TaskKind, TaskSpec, WorkerId,
    WorkerProfile,
};
use nova_matcher::Matcher;
use nova_ports::{
    CapacityLedger, Clock, FilterExpr, IdempotencyGate, PolicySandbox, Reservation, TaskStore,
};
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "config/home.toml")]
    config: PathBuf,
}

#[derive(Debug, Deserialize, Clone)]
struct Config {
    /// Logical region id for sim / ops (e.g. home, edge-b).
    #[serde(default = "default_region")]
    region: String,
    role: String,
    listen: String,
    /// When role=edge, forward SUBMIT here (authoritative home).
    #[serde(default)]
    home_upstream: Option<String>,
    #[serde(default = "default_threshold")]
    pending_threshold: usize,
}

fn default_region() -> String {
    "default".into()
}

fn default_threshold() -> usize {
    10_000
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let text = std::fs::read_to_string(&args.config)
        .with_context(|| format!("config {}", args.config.display()))?;
    let cfg: Config = toml::from_str(&text)?;
    let addr: SocketAddr = cfg.listen.parse()?;

    let world = MemWorld::new();
    let claim = Arc::new(ClaimService::new(
        world.store.clone() as Arc<dyn TaskStore>,
        world.ledger.clone() as Arc<dyn CapacityLedger>,
        world.sandbox.clone() as Arc<dyn PolicySandbox>,
        world.clock.clone() as Arc<dyn Clock>,
        Matcher::new("server"),
        cfg.pending_threshold,
    ));

    let listener = TcpListener::bind(addr).await?;
    info!(
        region = %cfg.region,
        role = %cfg.role,
        %addr,
        upstream = ?cfg.home_upstream,
        "nova-server listening"
    );

    if let Some(parent) = args.config.parent() {
        let marker = parent.join(format!(".ready-{}", cfg.region));
        let _ = std::fs::write(&marker, b"ok");
    }

    loop {
        let (mut sock, peer) = listener.accept().await?;
        let claim = claim.clone();
        let world = world.clone();
        let cfg = cfg.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(&mut sock, &cfg, claim, world).await {
                warn!(%peer, error = %e, "conn error");
            }
        });
    }
}

async fn handle_conn(
    sock: &mut TcpStream,
    cfg: &Config,
    claim: Arc<ClaimService>,
    world: MemWorld,
) -> Result<()> {
    let mut buf = vec![0u8; 4096];
    let n = sock.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }
    let line = std::str::from_utf8(&buf[..n])?.trim();
    let parts: Vec<_> = line.split_whitespace().collect();
    let resp = dispatch(&parts, cfg, claim, world).await?;
    sock.write_all(resp.as_bytes()).await?;
    Ok(())
}

async fn dispatch(
    parts: &[&str],
    cfg: &Config,
    claim: Arc<ClaimService>,
    world: MemWorld,
) -> Result<String> {
    match parts {
        ["HEALTH"] => Ok(format!("OK region={} role={}\n", cfg.region, cfg.role)),
        ["STATS"] => {
            let pending = world
                .store
                .list_candidates(&FilterExpr {
                    pending_only: true,
                    limit: 100_000,
                })
                .await?
                .len();
            Ok(format!(
                "OK region={} role={} pending={}\n",
                cfg.region, cfg.role, pending
            ))
        }
        ["SUBMIT", kind, units] => {
            if cfg.role == "edge" {
                let Some(up) = &cfg.home_upstream else {
                    return Ok("ERR edge_missing_home_upstream\n".into());
                };
                return forward(up, &format!("SUBMIT {kind} {units}\n")).await;
            }
            if cfg.role != "home" {
                return Ok("ERR role_cannot_submit\n".into());
            }
            submit_local(&claim, &world, kind, units).await
        }
        ["CLAIM", worker_hex, cap] if cfg.role == "home" => {
            let wid = WorkerId(uuid_from_str(worker_hex)?);
            let cap: u32 = cap.parse()?;
            let _ = world.ledger.register_worker(wid, cap).await;
            let remaining = world.ledger.remaining(&wid).await.unwrap_or(cap);
            let profile = WorkerProfile {
                id: wid,
                total_capacity: cap,
                remaining_capacity: remaining,
                labels: vec![],
            };
            match claim.claim_one(&profile).await {
                Ok((task, attempt)) => {
                    let kind = match task.kind {
                        TaskKind::AigcImage => "aigc_image",
                        TaskKind::AigcVideo => "aigc_video",
                        TaskKind::Agent => "agent",
                    };
                    Ok(format!(
                        "OK {} {} {} {}\n",
                        task.id.0, attempt.0, task.capacity.units, kind
                    ))
                }
                Err(_) => Ok("ERR no_eligible\n".into()),
            }
        }
        ["COMPLETE", task_hex, attempt, worker_hex, units, success]
            if cfg.role == "home" =>
        {
            let tid = TaskId(uuid_from_str(task_hex)?);
            let attempt = Attempt(attempt.parse()?);
            let wid = WorkerId(uuid_from_str(worker_hex)?);
            let units: u32 = units.parse()?;
            let success = *success == "1" || *success == "true";
            claim
                .complete(&tid, &wid, attempt, units, success)
                .await
                .ok();
            Ok("OK\n".into())
        }
        ["GET", task_hex] => {
            let tid = TaskId(uuid_from_str(task_hex)?);
            match world.store.get(&tid).await? {
                Some(rec) => Ok(format!(
                    "OK state={:?} attempt={} owner={:?}\n",
                    rec.state,
                    rec.attempt.0,
                    rec.owner.map(|w| w.0)
                )),
                None => Ok("ERR not_found\n".into()),
            }
        }
        ["LIST"] if cfg.role == "home" => {
            let recs = world.store.list_all(5_000).await?;
            let items: Vec<serde_json::Value> = recs
                .into_iter()
                .map(|r| {
                    let kind = match r.spec.kind {
                        TaskKind::AigcImage => "aigc_image",
                        TaskKind::AigcVideo => "aigc_video",
                        TaskKind::Agent => "agent",
                    };
                    serde_json::json!({
                        "id": r.spec.id.0.to_string(),
                        "kind": kind,
                        "units": r.spec.capacity.units,
                        "state": format!("{:?}", r.state).to_lowercase(),
                        "attempt": r.attempt.0,
                        "owner": r.owner.map(|w| w.0.to_string()),
                    })
                })
                .collect();
            Ok(format!("OK {}\n", serde_json::to_string(&items)?))
        }
        _ => Ok("ERR unknown\n".into()),
    }
}

async fn submit_local(
    claim: &ClaimService,
    world: &MemWorld,
    kind: &str,
    units: &str,
) -> Result<String> {
    let units: u32 = units.parse()?;
    let kind = match kind {
        "aigc_image" => TaskKind::AigcImage,
        "aigc_video" => TaskKind::AigcVideo,
        _ => TaskKind::Agent,
    };
    if claim.submit_allowed().await.is_err() {
        return Ok("ERR backpressure\n".into());
    }
    let id = TaskId::new();
    let key = IdempotencyKey(id.0.to_string());
    match world.gate.reserve(&key).await? {
        Reservation::AlreadyExists => Ok("ERR idempotent\n".into()),
        Reservation::Reserved => {
            let now = world.clock.now_ms().await;
            world
                .store
                .insert(TaskSpec {
                    id,
                    kind,
                    priority: Priority::Normal,
                    capacity: CapacityNeed { units },
                    submitted_at_ms: now,
                    predicate: String::new(),
                })
                .await?;
            Ok(format!("OK {}\n", id.0))
        }
    }
}

async fn forward(addr: &str, line: &str) -> Result<String> {
    let mut stream = TcpStream::connect(addr).await?;
    stream.write_all(line.as_bytes()).await?;
    let mut buf = vec![0u8; 1024];
    let n = stream.read(&mut buf).await?;
    Ok(std::str::from_utf8(&buf[..n])?.to_string())
}

fn uuid_from_str(s: &str) -> Result<uuid::Uuid> {
    Ok(uuid::Uuid::parse_str(s)?)
}
