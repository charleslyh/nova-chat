//! L2 multi-process scenarios: HTTP API + shared Trace + simple var capture.

use std::collections::HashMap;
use std::path::Path;
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
}

fn default_timeout() -> u64 {
    8_000
}
fn default_interval() -> u64 {
    200
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
    for p in paths {
        let name = run_one(&p)
            .await
            .with_context(|| format!("l2 scenario {}", p.display()))?;
        names.push(name);
    }
    Ok(names)
}

async fn run_one(path: &Path) -> Result<String> {
    let text = std::fs::read_to_string(path)?;
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
