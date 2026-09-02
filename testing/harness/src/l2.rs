//! L2 multi-process scenarios: HTTP API + shared Trace + simple var capture.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::Value;

use crate::oracle::run_oracles;
use crate::trace::{Trace, TraceEvent};

#[derive(Debug, Deserialize)]
struct ScenarioFile {
    name: String,
    #[serde(default)]
    covers: Vec<String>,
    #[serde(default)]
    oracles: Vec<String>,
    #[serde(default = "default_true")]
    trace: bool,
    /// Whether this scenario leaves a node unusable (stopped or killed).
    ///
    /// Declared here rather than encoded in the filename. The suite previously
    /// relied on `zz-` / `zzz-` prefixes to keep such scenarios last, which meant
    /// a correctly named new scenario could still be stranded by an unluckily
    /// sorted one — and nothing would explain why it failed. The runner now
    /// orders by this flag, so the ordering requirement is enforced instead of
    /// merely documented.
    #[serde(default)]
    destructive: bool,
    /// Nodes this scenario needs to be serving. Checked before the steps run so a
    /// scenario stranded by an earlier one reports *that* instead of failing on
    /// an unrelated assertion further down.
    #[serde(default)]
    requires_nodes: Vec<u16>,
    #[serde(default)]
    steps: Vec<Step>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum Step {
    HttpGet {
        url: String,
        expect_contains: Option<String>,
        #[serde(default)]
        expect_status: Option<u16>,
        #[serde(default)]
        capture: HashMap<String, String>,
    },
    HttpPost {
        url: String,
        body: Option<String>,
        expect_contains: Option<String>,
        #[serde(default)]
        expect_status: Option<u16>,
        #[serde(default)]
        capture: HashMap<String, String>,
    },
    SleepMs {
        ms: u64,
    },
    /// Poll until expect_contains appears or timeout.
    HttpGetUntil {
        url: String,
        expect_contains: String,
        #[serde(default = "default_timeout")]
        timeout_ms: u64,
        #[serde(default = "default_interval")]
        interval_ms: u64,
    },
    /// Collect SSE `id`/`data` events from a live stream, then disconnect.
    HttpSseCollect {
        url: String,
        #[serde(default = "default_sse_min")]
        expect_min_events: usize,
        /// Stop after this many events (default: expect_min_events).
        #[serde(default)]
        max_events: Option<usize>,
        #[serde(default = "default_sse_timeout")]
        timeout_ms: u64,
        /// Var name for the last `sequence_number` received; also sets
        /// `{name}_next` = last + 1 for use as an inclusive resume point.
        ///
        /// The SSE `id:` field carries the sequence number, so a reconnect can
        /// resume purely from what the client observed (INV-12).
        #[serde(default)]
        capture_last_seq: Option<String>,
    },
    HttpDelete {
        url: String,
        #[serde(default)]
        expect_status: Option<u16>,
    },
    /// Kill whatever is listening on a TCP port (abrupt failure).
    KillListener {
        port: u16,
    },
    /// Send SIGTERM and wait, so graceful drain can actually be observed.
    ///
    /// Distinct from `KillListener` on purpose: drain is the mechanism that makes
    /// rolling deploys free, and a SIGKILL would bypass it entirely (D21).
    GracefulStop {
        port: u16,
        #[serde(default = "default_drain_wait")]
        wait_ms: u64,
    },
}

fn default_drain_wait() -> u64 {
    3_000
}

fn default_timeout() -> u64 {
    8_000
}
fn default_interval() -> u64 {
    200
}
fn default_sse_min() -> usize {
    1
}
fn default_sse_timeout() -> u64 {
    10_000
}

pub async fn run_l2_dir(dir: &Path) -> Result<Vec<String>> {
    let mut names = Vec::new();
    if !dir.exists() {
        return Ok(names);
    }
    let mut paths: Vec<_> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("yaml"))
        .collect();
    paths.sort();

    // Non-destructive scenarios first, destructive ones last, each group keeping
    // filename order for reproducibility. Reading the flag needs a parse, so a
    // malformed file surfaces here rather than midway through the run.
    let mut ordered: Vec<(bool, PathBuf)> = Vec::new();
    for p in paths {
        let text = std::fs::read_to_string(&p)?;
        if is_blank_scenario(&text) {
            eprintln!("  {} ... skipped (no content)", p.display());
            continue;
        }
        let sc: ScenarioFile = serde_yaml::from_str(&text)
            .with_context(|| format!("parsing {}", p.display()))?;
        ordered.push((sc.destructive, p));
    }
    ordered.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    for (_, p) in ordered {
        let name = run_one(&p)
            .await
            .with_context(|| format!("l2 scenario {}", p.display()))?;
        if !name.is_empty() {
            names.push(name);
        }
    }
    Ok(names)
}

async fn run_one(path: &Path) -> Result<String> {
    let text = std::fs::read_to_string(path)?;
    if is_blank_scenario(&text) {
        eprintln!("  {} ... skipped (no content)", path.display());
        return Ok(String::new());
    }
    let sc: ScenarioFile = serde_yaml::from_str(&text)?;
    let name = sc.name.clone();
    eprint!("  {name} ... ");
    let mut trace = if sc.trace {
        Trace::with_jsonl_file(&sc.name, Path::new("testing/reports/traces"))?
    } else {
        Trace::new(&sc.name)
    };
    let mut now_ms = 1u64;
    let mut vars: HashMap<String, String> = HashMap::new();

    for port in &sc.requires_nodes {
        let addr = format!("127.0.0.1:{port}");
        if tokio::net::TcpStream::connect(&addr).await.is_err() {
            bail!(
                "{}: requires a node on {addr}, but nothing is listening. A destructive \
                 scenario ran earlier and the fixture is not restarted between scenarios; \
                 mark the scenario that stops this node with `destructive: true`.",
                sc.name
            );
        }
    }

    for step in sc.steps {
        match step {
            Step::HttpGet {
                url,
                expect_contains,
                expect_status,
                capture,
            } => {
                let url = subst(&url, &vars);
                let resp = http("GET", &url, None).await?;
                check_status(&sc.name, "GET", &url, &resp, expect_status)?;
                apply_capture(&resp, &capture, &mut vars)?;
                let expect = expect_contains.map(|s| subst(&s, &vars));
                let ok = expect.as_ref().map(|s| resp.contains(s)).unwrap_or(true);
                trace.push(TraceEvent::ApiCall {
                    method: "GET".into(),
                    url: url.clone(),
                    status_ok: ok,
                    detail: resp.chars().take(240).collect(),
                    at_ms: now_ms,
                });
                if !ok {
                    bail!("{}: GET {url} missing {:?}", sc.name, expect);
                }
            }
            Step::HttpPost {
                url,
                body,
                expect_contains,
                expect_status,
                capture,
            } => {
                let url = subst(&url, &vars);
                let body = body.map(|b| subst(&b, &vars));
                let resp = http("POST", &url, body.as_deref()).await?;
                check_status(&sc.name, "POST", &url, &resp, expect_status)?;
                apply_capture(&resp, &capture, &mut vars)?;
                let expect = expect_contains.map(|s| subst(&s, &vars));
                let ok = expect.as_ref().map(|s| resp.contains(s)).unwrap_or(true);
                trace.push(TraceEvent::ApiCall {
                    method: "POST".into(),
                    url: url.clone(),
                    status_ok: ok,
                    detail: resp.chars().take(240).collect(),
                    at_ms: now_ms,
                });
                if !ok {
                    bail!("{}: POST {url} missing {:?}", sc.name, expect);
                }
            }
            Step::SleepMs { ms } => {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                now_ms = now_ms.saturating_add(ms);
                trace.push(TraceEvent::Clock { now_ms });
                continue;
            }
            Step::HttpGetUntil {
                url,
                expect_contains,
                timeout_ms,
                interval_ms,
            } => {
                let url = subst(&url, &vars);
                let needle = subst(&expect_contains, &vars);
                let start = std::time::Instant::now();
                let last;
                loop {
                    let resp = http("GET", &url, None).await?;
                    if resp.contains(&needle) {
                        last = resp;
                        trace.push(TraceEvent::ApiCall {
                            method: "GET_UNTIL".into(),
                            url: url.clone(),
                            status_ok: true,
                            detail: last.chars().take(240).collect(),
                            at_ms: now_ms,
                        });
                        break;
                    }
                    if start.elapsed() > Duration::from_millis(timeout_ms) {
                        trace.push(TraceEvent::ApiCall {
                            method: "GET_UNTIL".into(),
                            url: url.clone(),
                            status_ok: false,
                            detail: resp.chars().take(240).collect(),
                            at_ms: now_ms,
                        });
                        bail!(
                            "{}: timeout waiting for {needle:?} on {url}",
                            sc.name
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                }
                let _ = last;
            }
            Step::HttpSseCollect {
                url,
                expect_min_events,
                max_events,
                timeout_ms,
                capture_last_seq,
            } => {
                let url = subst(&url, &vars);
                let cap = max_events.unwrap_or(expect_min_events).max(expect_min_events);
                let (ids, bodies) = sse_collect(&url, cap, Duration::from_millis(timeout_ms))
                    .await
                    .with_context(|| format!("{}: sse {url}", sc.name))?;
                if ids.len() < expect_min_events {
                    bail!(
                        "{}: sse {url} got {} events, want >= {expect_min_events}",
                        sc.name,
                        ids.len()
                    );
                }
                if let Some(var) = capture_last_seq {
                    let last = *ids.last().unwrap();
                    vars.insert(var.clone(), last.to_string());
                    vars.insert(format!("{var}_next"), (last + 1).to_string());
                }
                trace.push(TraceEvent::ApiCall {
                    method: "SSE_COLLECT".into(),
                    url: url.clone(),
                    status_ok: true,
                    detail: format!(
                        "events={} last_seq={} sample={}",
                        ids.len(),
                        ids.last().copied().unwrap_or(0),
                        bodies.last().map(|s| s.chars().take(80).collect::<String>()).unwrap_or_default()
                    ),
                    at_ms: now_ms,
                });
            }
            Step::HttpDelete { url, expect_status } => {
                let url = subst(&url, &vars);
                let resp = http_delete(&url).await?;
                check_status(&sc.name, "DELETE", &url, &resp, expect_status)?;
                trace.push(TraceEvent::ApiCall {
                    method: "DELETE".into(),
                    url: url.clone(),
                    status_ok: http_status(&resp).map(|s| s < 400).unwrap_or(false),
                    detail: http_status(&resp).map(|s| s.to_string()).unwrap_or_default(),
                    at_ms: now_ms,
                });
            }
            Step::KillListener { port } => {
                kill_listener(port)?;
                trace.push(TraceEvent::FaultInjected {
                    kind: "kill_listener".into(),
                    target: format!("port:{port}"),
                    at_ms: now_ms,
                });
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Step::GracefulStop { port, wait_ms } => {
                signal_listener(port, "TERM")?;
                trace.push(TraceEvent::DrainStarted {
                    node_tag: format!("port:{port}"),
                    in_flight: 0,
                    at_ms: now_ms,
                });
                tokio::time::sleep(Duration::from_millis(wait_ms)).await;
            }
        }
        now_ms += 1;
        trace.push(TraceEvent::Clock { now_ms });
    }

    let _ = sc.covers;
    run_oracles(&trace, &sc.oracles)?;
    eprintln!("ok");
    Ok(name)
}

fn http_status(resp: &str) -> Option<u16> {
    resp.lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

fn check_status(
    scenario: &str,
    method: &str,
    url: &str,
    resp: &str,
    expect_status: Option<u16>,
) -> Result<()> {
    let Some(want) = expect_status else {
        return Ok(());
    };
    let Some(got) = http_status(resp) else {
        bail!("{scenario}: {method} {url} missing HTTP status line");
    };
    if got != want {
        bail!("{scenario}: {method} {url} expect status {want} got {got}");
    }
    Ok(())
}

fn subst(s: &str, vars: &HashMap<String, String>) -> String {
    let mut out = s.to_string();
    for (k, v) in vars {
        out = out.replace(&format!("{{{{{k}}}}}"), v);
    }
    out
}

fn http_body(resp: &str) -> &str {
    resp.split("\r\n\r\n")
        .nth(1)
        .or_else(|| resp.split("\n\n").nth(1))
        .unwrap_or(resp)
}

fn apply_capture(
    resp: &str,
    capture: &HashMap<String, String>,
    vars: &mut HashMap<String, String>,
) -> Result<()> {
    if capture.is_empty() {
        return Ok(());
    }
    let body = http_body(resp);
    let v: Value = serde_json::from_str(body.trim())
        .with_context(|| format!("capture: not json body: {}", body.chars().take(120).collect::<String>()))?;
    for (var, json_key) in capture {
        let Some(field) = v.get(json_key) else {
            bail!("capture: missing json key {json_key}");
        };
        let s = match field {
            Value::String(s) => s.clone(),
            other => other.to_string().trim_matches('"').to_string(),
        };
        vars.insert(var.clone(), s);
    }
    Ok(())
}

/// Abrupt termination: models an unplanned crash.
///
/// Uses SIGKILL specifically. Sending SIGTERM here would invoke graceful drain
/// and the scenario would then verify the *opposite* of what it intends.
fn kill_listener(port: u16) -> Result<()> {
    signal_listener(port, "KILL")
}

/// Send a signal to whatever is listening on `port`.
fn signal_listener(port: u16, signal: &str) -> Result<()> {
    use std::process::Command;
    let out = Command::new("lsof")
        .args(["-nP", &format!("-iTCP:{port}"), "-sTCP:LISTEN", "-t"])
        .output()
        .with_context(|| format!("lsof port {port}"))?;
    if !out.status.success() {
        // Nothing listening: the step's intent is already satisfied.
        return Ok(());
    }
    for pid in String::from_utf8_lossy(&out.stdout).split_whitespace() {
        let st = Command::new("kill")
            .args([&format!("-{signal}"), pid])
            .status()?;
        if !st.success() {
            bail!("kill -{signal} {pid} failed");
        }
    }
    Ok(())
}

async fn http_delete(url: &str) -> Result<String> {
    http("DELETE", url, None).await
}

async fn http(method: &str, url: &str, body: Option<&str>) -> Result<String> {
    let rest = url.trim_start_matches("http://");
    let (addr, path) = rest
        .split_once('/')
        .map(|(a, p)| (a, format!("/{p}")))
        .unwrap_or((rest, "/".into()));
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr).await?;
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

/// Read up to `max_events` SSE events from `url`, then drop the connection.
async fn sse_collect(url: &str, max_events: usize, timeout: Duration) -> Result<(Vec<u64>, Vec<String>)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let rest = url.trim_start_matches("http://");
    let (addr, path) = rest
        .split_once('/')
        .map(|(a, p)| (a, format!("/{p}")))
        .unwrap_or((rest, "/".into()));
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .with_context(|| format!("connect {addr}"))?;
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nAccept: text/event-stream\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await?;

    let deadline = tokio::time::Instant::now() + timeout;
    let mut raw = Vec::new();
    let mut ids = Vec::new();
    let mut bodies = Vec::new();
    let mut header_done = false;

    while ids.len() < max_events {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            break;
        }
        let mut chunk = [0u8; 2048];
        let n = match tokio::time::timeout(left, stream.read(&mut chunk)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => break,
        };
        raw.extend_from_slice(&chunk[..n]);

        if !header_done {
            let text = String::from_utf8_lossy(&raw);
            let Some(pos) = text.find("\r\n\r\n").or_else(|| text.find("\n\n")) else {
                continue;
            };
            let sep = if text[pos..].starts_with("\r\n\r\n") {
                4
            } else {
                2
            };
            let status_line = text.lines().next().unwrap_or("");
            if !status_line.contains("200") {
                bail!("sse status not 200: {status_line}");
            }
            raw = text[pos + sep..].as_bytes().to_vec();
            header_done = true;
        }

        let text = String::from_utf8_lossy(&raw);
        let mut start = 0usize;
        while let Some(rel) = text[start..].find("\n\n") {
            let end = start + rel;
            let frame = text[start..end].trim_start_matches('\r');
            start = end + 2;
            let mut id = None;
            let mut data = String::new();
            for line in frame.lines() {
                let line = line.trim_end_matches('\r');
                if let Some(v) = line.strip_prefix("id:") {
                    id = v.trim().parse().ok();
                } else if let Some(v) = line.strip_prefix("data:") {
                    if !data.is_empty() {
                        data.push('\n');
                    }
                    data.push_str(v.trim_start());
                }
            }
            if let Some(id) = id {
                ids.push(id);
                bodies.push(data);
                if ids.len() >= max_events {
                    break;
                }
            }
        }
        if start > 0 {
            raw = text[start..].as_bytes().to_vec();
        }
    }
    Ok((ids, bodies))
}

/// Whether a scenario file carries no actual content.
///
/// Comment-only files exist during migrations as tombstones for scenarios that
/// were withdrawn; treating them as parse errors would block the whole run for
/// no benefit.
fn is_blank_scenario(text: &str) -> bool {
    text.lines()
        .map(str::trim)
        .all(|line| line.is_empty() || line.starts_with('#'))
}
