use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Verify {
        #[arg(long)]
        level: String,
    },
    Procs {
        action: String,
    },
    Coverage,
    Unittest,
    CheckDeps,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "error".into()),
        )
        .init();
    let args = Args::parse();
    match args.cmd {
        Cmd::Verify { level } => verify(&level).await?,
        Cmd::Procs { action } => {
            procs(&action).await?;
            println!("procs {action}");
        }
        Cmd::Coverage => coverage().await?,
        Cmd::Unittest => unittest()?,
        Cmd::CheckDeps => check_deps()?,
    }
    Ok(())
}

async fn verify(level: &str) -> Result<()> {
    match level {
        "l0" => {
            println!("verify l0");
            conformance::run_mem_suite_reported().await;
            println!("verify l0 OK");
        }
        "l1" => {
            println!("verify l1");
            let _names = harness::run_l1_dir(Path::new("verify/scenarios/l1")).await?;
            println!("verify l1 OK");
        }
        "l2" => {
            println!("verify l2");
            eprint!("  procs up ... ");
            procs("up").await?;
            eprintln!("ok");
            let _names = harness::run_l2_dir(Path::new("verify/scenarios/l2")).await?;
            eprint!("  procs down ... ");
            procs("down").await?;
            eprintln!("ok");
            println!("verify l2 OK");
        }
        "l4" => verify_l4().await?,
        other => bail!("unknown level {other}"),
    }
    Ok(())
}

/// L4: the official Python SDK driving the conversation endpoints (D27).
///
/// Self-skipping: the SDK is a supplementary check, not a hard gate. A machine
/// without `python3` or the `openai` package must not fail `just verify` — the
/// claim being verified is "an unmodified official client works", and it is
/// meaningless to *fail* a machine that merely lacks the client.
async fn verify_l4() -> Result<()> {
    println!("verify l4");

    if !python_has_openai().await {
        println!("verify l4 SKIPPED (python3 or the `openai` package is unavailable)");
        return Ok(());
    }

    eprint!("  procs up ... ");
    procs("up").await?;
    eprintln!("ok");
    let result = run_sdk_compat().await;
    eprint!("  procs down ... ");
    procs("down").await?;
    eprintln!("ok");

    let status = result?;
    if status != 0 {
        bail!("verify l4 FAILED: verify/sdk-compat/run.py exited {status}");
    }
    println!("verify l4 OK");
    Ok(())
}

/// Whether `python3` can import the `openai` package.
///
/// Checked with an import, not a package-manager probe: `pip show openai` can
/// report a broken install, and a different interpreter than the one `python3`
/// resolves to would go unnoticed.
async fn python_has_openai() -> bool {
    tokio::process::Command::new("python3")
        .args(["-c", "import openai"])
        .output()
        .await
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Run the SDK-compat script against the local fixture and return its exit code.
async fn run_sdk_compat() -> Result<i32> {
    let out = tokio::process::Command::new("python3")
        .arg("verify/sdk-compat/run.py")
        .env("NOVA_GATEWAY_URL", "http://127.0.0.1:18080/v1")
        .output()
        .await
        .context("spawning python3 for the SDK-compat layer")?;

    // The script is the only thing that knows what it asserted; surface its
    // output in both outcomes so a passing run is visible and a failing one
    // carries its own diagnosis.
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    if !stdout.is_empty() {
        print!("{}", stdout);
    }
    if !out.status.success() {
        eprint!("{}", stderr);
    }
    Ok(out.status.code().unwrap_or(1))
}

async fn procs(action: &str) -> Result<()> {
    let run_dir = PathBuf::from("target/nova-procs");
    std::fs::create_dir_all(&run_dir)?;
    match action {
        "up" => {
            procs_down(&run_dir)?;
            // A leftover sim or gateway may still hold these ports; clear them
            // before starting the L2 fixture.
            kill_listeners(&[18080, 18081, 18082, 19000, 19001])?;

            // 1. Shared in-memory carrier: data plane (19000) + control plane (19001).
            start_bin(
                "mock-server",
                &[
                    "--listen",
                    "127.0.0.1:19000",
                    "--control-listen",
                    "127.0.0.1:19001",
                ],
                run_dir.join("mem-server.pid"),
            )?;
            wait_port("127.0.0.1:19000", Duration::from_secs(20)).await?;
            wait_port("127.0.0.1:19001", Duration::from_secs(20)).await?;

            // 2. Standalone sweep process: single owner of reap/retention/expiry.
            //    Short heartbeat TTL so lost-claim recovery is observable in tests.
            start_bin(
                "mock-sweep",
                &["--heartbeat-ttl-ms", "2000", "--retain-after-terminal-ms", "60000"],
                run_dir.join("sweep.pid"),
            )?;

            // 3. Execution daemon over the shared carrier, with a scripted scheduler
            //    (normal answers plus a `hang` rule for overload scenarios).
            start_bin(
                "mock-agentd",
                &[
                    "--scheduler",
                    "scripted",
                    "--scheduler-script",
                    "verify/config/l2-agent-script.yaml",
                    // The sweep fixture reaps at 2000ms; the heartbeat must fire
                    // well inside that or any generation the agent does not finish
                    // instantly gets reaped mid-flight (see the default of 30s
                    // against a 2s TTL, which is exactly the mismatch that made
                    // background-then-subscribe flake).
                    "--heartbeat-interval-ms",
                    "500",
                ],
                run_dir.join("agentd.pid"),
            )?;

            // 4. Three peer gateways over the shared carrier (no embedded execution).
            for (tag, port) in [("node-a", 18080), ("node-b", 18081), ("node-c", 18082)] {
                start_bin(
                    "nova-responses-gateway",
                    &["--config", &format!("verify/config/{tag}.toml")],
                    run_dir.join(format!("{tag}.pid")),
                )?;
                let _ = port;
            }
            wait_port("127.0.0.1:18080", Duration::from_secs(20)).await?;
            wait_port("127.0.0.1:18081", Duration::from_secs(20)).await?;
            wait_port("127.0.0.1:18082", Duration::from_secs(20)).await?;
        }
        "down" => {
            procs_down(&run_dir)?;
            kill_listeners(&[18080, 18081, 18082, 19000, 19001])?;
        }
        other => bail!("unknown procs action {other}"),
    }
    Ok(())
}

fn procs_down(run_dir: &Path) -> Result<()> {
    for name in [
        "node-a.pid",
        "node-b.pid",
        "node-c.pid",
        "mem-server.pid",
        "sweep.pid",
        "agentd.pid",
    ] {
        kill_pidfile(&run_dir.join(name))?;
    }
    Ok(())
}

/// Fixture-only secrets for the local L2 harness.
///
/// Passed as environment variables rather than written into the config files,
/// because that is the only channel the service reads them from (SEC-4). These
/// values are for verification and must never appear in a deployment.
const FIXTURE_ENV: &[(&str, &str)] = &[
    // mem-server 校验完整性时需要；仅在验证夹具使用，绝不出现于部署。
    ("NOVA_INTEGRITY_KEY", "l2-fixture-integrity-key-0123456789"),
    // 共享载体的数据面地址；gateway / agentd / sweep 都从这里连。
    ("NOVA_MEM_SERVER_URL", "http://127.0.0.1:19000"),
];

fn start_bin(bin: &str, args: &[&str], pidfile: PathBuf) -> Result<()> {
    let status = Command::new("cargo")
        .args(["build", "--bin", bin])
        .status()?;
    if !status.success() {
        bail!("build {bin} failed");
    }
    let exe = PathBuf::from(format!("target/debug/{bin}"));
    let mut cmd = Command::new(&exe);
    for (key, value) in FIXTURE_ENV {
        cmd.env(key, value);
    }
    let child = cmd
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("spawn {bin}"))?;
    std::fs::write(&pidfile, child.id().to_string())?;
    std::mem::forget(child);
    Ok(())
}

fn kill_pidfile(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    if let Ok(pid_str) = std::fs::read_to_string(path) {
        if let Ok(pid) = pid_str.trim().parse::<i32>() {
            // SIGKILL for the same reason as `kill_listeners`: teardown must not
            // depend on drain completing.
            let _ = Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .status();
        }
    }
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// Free the fixture ports unconditionally.
///
/// Uses SIGKILL deliberately. SIGTERM would start graceful drain, and a node
/// holding an in-flight response that can never complete (no agent attached)
/// would then linger for the whole drain budget and keep the port bound — which
/// is exactly how a previous run left node-b occupied and made the next run fail
/// with a confusing 503. Teardown has to be deterministic.
fn kill_listeners(ports: &[u16]) -> Result<()> {
    for port in ports {
        let out = Command::new("lsof")
            .args(["-nP", &format!("-iTCP:{port}"), "-sTCP:LISTEN", "-t"])
            .output();
        let Ok(out) = out else { continue };
        if !out.status.success() {
            continue;
        }
        for pid in String::from_utf8_lossy(&out.stdout).split_whitespace() {
            let _ = Command::new("kill").args(["-KILL", pid]).status();
        }
    }
    // Brief settle so the port is actually released before we rebind.
    std::thread::sleep(Duration::from_millis(300));
    Ok(())
}

async fn wait_port(addr: &str, timeout: Duration) -> Result<()> {
    let start = std::time::Instant::now();
    loop {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return Ok(());
        }
        if start.elapsed() > timeout {
            bail!("timeout waiting for {addr}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn unittest() -> Result<()> {
    println!("unittest");
    let meta = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .output()
        .context("cargo metadata")?;
    if !meta.status.success() {
        bail!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&meta.stderr)
        );
    }
    let v: serde_json::Value =
        serde_json::from_slice(&meta.stdout).context("parse cargo metadata")?;
    let mut pkgs: Vec<String> = v["packages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|p| {
            p["targets"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|t| {
                    t["kind"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .any(|k| k.as_str() == Some("lib"))
                })
        })
        .filter_map(|p| p["name"].as_str().map(str::to_string))
        .collect();
    pkgs.sort();
    if pkgs.is_empty() {
        bail!("no lib packages in workspace");
    }

    let mut failed = false;
    for p in pkgs {
        let out = Command::new("cargo")
            .args(["test", "-q", "-p", &p, "--lib", "--", "--format=pretty"])
            .output()
            .with_context(|| format!("cargo test -p {p}"))?;
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        let cases: Vec<&str> = text.lines().filter(|l| is_test_case_line(l)).collect();
        if cases.is_empty() {
            if !out.status.success() {
                println!("  {p}");
                println!("    FAIL");
                for line in text.lines() {
                    println!("    {line}");
                }
                failed = true;
            }
            continue;
        }
        println!("  {p}");
        for c in &cases {
            println!("    {c}");
        }
        if !out.status.success() {
            println!("    FAIL");
            for line in text.lines() {
                if line.starts_with("failures:")
                    || line.starts_with("---- ")
                    || line.starts_with("error[")
                    || line.starts_with("thread ")
                    || line.starts_with("test result:")
                {
                    println!("    {line}");
                }
            }
            failed = true;
        }
    }
    if failed {
        bail!("unittest FAIL");
    }
    println!("unittest OK");
    Ok(())
}

fn is_test_case_line(line: &str) -> bool {
    let Some(rest) = line.strip_prefix("test ") else {
        return false;
    };
    rest.contains(" ... ")
}

/// Requirement ids declared by the gateway HTTP contract tests.
///
/// Each test that substantiates a requirement says so with a `/// covers:` doc
/// comment (e.g. `/// covers: FR-17, INV-41`). The declaration lives on the test
/// itself, mirroring conformance's `cases()`, so a test that stops asserting a
/// requirement stops claiming it — there is no second list here to drift. The
/// `covers_claims_are_substantiated` test inside that file enforces the ids are
/// actually asserted.
fn http_contract_covers() -> Result<Vec<String>> {
    let src = std::fs::read_to_string("gateway/tests/http_contract.rs")
        .context("gateway/tests/http_contract.rs")?;
    let mut ids = Vec::new();
    for line in src.lines() {
        let Some(rest) = line.trim().strip_prefix("/// covers:") else {
            continue;
        };
        for id in rest.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            ids.push(id.to_string());
        }
    }
    Ok(ids)
}

/// The requirement ids every automated gate must substantiate.
///
/// Follows `docs/requirements/spec.md` v3 and `docs/architecture/invariants.md`.
/// A single source of truth, shared by `coverage` (which demands each id be
/// substantiated) and `check_coverage_baseline_tracks_invariants` (which demands
/// every invariant in `invariants.md` appear here). Keeping the two in one list
/// means a newly added invariant cannot silently fall outside the coverage gate.
fn coverage_baseline() -> std::collections::BTreeSet<&'static str> {
    [
        // Lifecycle.
        "FR-1", "FR-2", "FR-3", "FR-4", "FR-5", "FR-6", "FR-7", "FR-8",
        // Streaming and resumption.
        "FR-9", "FR-10", "FR-11", "FR-12", "FR-13", "FR-14",
        // Storage and context. FR-22 (content expiry) is removed with D30:
        // durable content lives in the conversation snapshot, whose retention is
        // an operator-side concern (OR-5), not a per-response record to sweep.
        "FR-15", "FR-16", "FR-17", "FR-18", "FR-19", "FR-20", "FR-21",
        // Protocol subset.
        "FR-23", "FR-24", "FR-25", "FR-26", "FR-27", "FR-28",
        // Ingress and routing.
        "FR-29", "FR-30", "FR-31", "FR-32", "FR-33",
        // Reliability.
        "FR-34", "FR-35", "FR-36", "FR-37", "FR-38", "FR-39",
        // Conversation container (D28: the compatibility container plus the
        // event stream, turn lock and business events that were the D26 session
        // layer — merged into one resource).
        "FR-40", "FR-41", "FR-42", "FR-43", "FR-44", "FR-45",
        // Correctness.
        "CR-1", "CR-2", "CR-3", "CR-4", "CR-5", "CR-6", "CR-7", "CR-8", "CR-9", "CR-10",
        "CR-11", "CR-12", "CR-13", "CR-14", "CR-15", "CR-16",
        // Invariants still in force.
        "INV-1", "INV-2", "INV-5", "INV-6", "INV-11", "INV-12", "INV-16", "INV-29", "INV-30",
        "INV-32", "INV-34", "INV-35", "INV-40", "INV-41", "INV-42", "INV-43",
        "INV-44", "INV-45", "INV-46", "INV-47", "INV-48", "INV-49", "INV-50", "INV-51",
        "INV-52", "INV-54", "INV-55", "INV-56", "INV-57", "INV-58", "INV-59",
        // Security.
        "SEC-2", "SEC-3", "SEC-5", "SEC-6", "SEC-7",
    ]
    .into_iter()
    .collect()
}

async fn coverage() -> Result<()> {
    use std::collections::{BTreeMap, BTreeSet};

    let baseline = coverage_baseline();

    // L0 coverage is obtained by **running the contract and asking what it
    // substantiated**, not from a literal list maintained alongside it.
    //
    // The previous shape kept a third hard-coded copy of these ids here, so
    // deleting an assertion left the reported figure untouched — the gate could
    // not observe its own coverage shrinking. Now a removed case immediately
    // lowers the number, and a skipped optional port contributes nothing.
    let l0 = conformance::run_mem_suite().await;
    let mut covered: BTreeSet<String> = l0.covered().into_iter().map(str::to_string).collect();

    // Static gates substantiate requirements too, and they report what they
    // cover rather than having it restated here.
    for id in check_protocol_spec_is_publishable()? {
        covered.insert((*id).to_string());
    }

    if !l0.skipped.is_empty() {
        println!("  l0 skipped    {} case(s):", l0.skipped.len());
        for (case, reason) in &l0.skipped {
            println!("    - {case}: {reason}");
        }
    }

    let mut scenario_covers: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut l1 = 0usize;
    let mut l2 = 0usize;

    for (label, dir) in [
        ("l1", "verify/scenarios/l1"),
        ("l2", "verify/scenarios/l2"),
    ] {
        let path = Path::new(dir);
        if !path.exists() {
            continue;
        }
        let mut files: Vec<_> = std::fs::read_dir(path)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("yaml"))
            .collect();
        files.sort();
        for f in files {
            let text = std::fs::read_to_string(&f)?;
            // Comment-only files are migration tombstones; the runners skip them,
            // so counting them here would overstate the suite size.
            if text
                .lines()
                .map(str::trim)
                .all(|line| line.is_empty() || line.starts_with('#'))
            {
                continue;
            }
            let name = yaml_scalar(&text, "name").unwrap_or_else(|| {
                f.file_stem().unwrap().to_string_lossy().into_owned()
            });
            let mut refs = yaml_seq(&text, "covers");
            for o in yaml_seq(&text, "oracles") {
                if let Some(oracle) = harness::builtin(&o) {
                    for c in oracle.covers() {
                        refs.push((*c).to_string());
                    }
                }
            }
            refs.sort();
            refs.dedup();
            for r in &refs {
                covered.insert(r.clone());
            }
            scenario_covers.insert(format!("{label}/{name}"), refs);
            match label {
                "l1" => l1 += 1,
                "l2" => l2 += 1,
                _ => {}
            }
        }
    }

    // Gateway HTTP contract tests declare their own covers via `/// covers:`
    // doc comments. Fold them in so service-layer properties — which the port
    // contract cannot observe — are still counted, sourced from the test that
    // substantiates them rather than from a second literal list here.
    for id in http_contract_covers()? {
        covered.insert(id);
    }

    // Requirements whose verification is intentionally deferred. Listing them
    // here keeps them visible in the report instead of quietly missing.
    //
    // The list is empty; the mechanism stays so a future withdrawn requirement
    // stays visible rather than silently dropping coverage.
    let deferred: BTreeSet<&str> = BTreeSet::new();

    let covered_baseline: BTreeSet<_> = covered
        .iter()
        .filter(|r| baseline.contains(r.as_str()))
        .cloned()
        .collect();
    let mut missing: Vec<_> = baseline
        .iter()
        .filter(|r| !covered_baseline.contains(**r))
        .copied()
        .collect();
    missing.sort_by_key(|r| req_sort_key(r));

    let cr_missing: Vec<_> = missing.iter().filter(|r| r.starts_with("CR-")).copied().collect();
    let fr_missing: Vec<_> = missing.iter().filter(|r| r.starts_with("FR-")).copied().collect();
    let inv_missing: Vec<_> = missing.iter().filter(|r| r.starts_with("INV-")).copied().collect();
    let gate_cr: Vec<_> = cr_missing
        .iter()
        .filter(|r| !deferred.contains(**r))
        .copied()
        .collect();
    let deferred_gaps: Vec<_> = missing
        .iter()
        .filter(|r| deferred.contains(*r))
        .copied()
        .collect();

    let pct = covered_baseline.len() * 100 / baseline.len();

    let backend_cases = conformance::cases()
        .iter()
        .filter(|c| matches!(c.scope, conformance::CaseScope::Backend))
        .count();
    let protocol_cases = conformance::cases()
        .iter()
        .filter(|c| matches!(c.scope, conformance::CaseScope::Protocol))
        .count();

    println!("coverage");
    println!(
        "  l0 cases      {} ran ({backend_cases} backend · {protocol_cases} protocol)",
        l0.passed.len()
    );
    println!("  scenarios     {l1} L1 · {l2} L2");
    println!(
        "  baseline hit  {}/{} ({}%)",
        covered_baseline.len(),
        baseline.len(),
        pct
    );
    if cr_missing.is_empty() {
        println!("  CR            all covered");
    } else {
        println!("  CR gaps       {}", cr_missing.join(", "));
    }
    if fr_missing.is_empty() {
        println!("  FR            all covered");
    } else {
        println!("  FR gaps       {}", fr_missing.join(", "));
    }
    if inv_missing.is_empty() {
        println!("  INV           all covered");
    } else {
        println!("  INV gaps      {}", inv_missing.join(", "));
    }
    if !deferred_gaps.is_empty() {
        println!(
            "  deferred      {} (本期范围外，不卡门禁)",
            deferred_gaps.join(", ")
        );
    }

    let verdict = if !gate_cr.is_empty() {
        "FAIL"
    } else if missing.iter().any(|r| !deferred.contains(r)) {
        "PARTIAL"
    } else if missing.is_empty() {
        "OK"
    } else {
        // 仅剩 deferred 缺口 → 当期范围内视为 OK
        "OK"
    };
    if verdict == "OK" && !deferred_gaps.is_empty() {
        println!("  note          in-scope complete; deferred still open");
    }
    println!("coverage {verdict}");

    std::fs::create_dir_all("verify/reports")?;
    let mut md = format!(
        "# Coverage detail\n\n**Verdict:** {verdict} — {}/{} ({pct}%)\n\n## Scenarios\n\n",
        covered_baseline.len(),
        baseline.len()
    );
    for (sc, refs) in &scenario_covers {
        md.push_str(&format!("- `{sc}`: {}\n", refs.join(", ")));
    }
    md.push_str("\n## Missing baseline\n\n");
    if missing.is_empty() {
        md.push_str("(none)\n");
    } else {
        for m in &missing {
            let tag = if deferred.contains(m) {
                " _(deferred)_"
            } else {
                ""
            };
            md.push_str(&format!("- `{m}`{tag}\n"));
        }
    }
    std::fs::write("verify/reports/traceability.md", &md)?;

    if verdict == "FAIL" {
        bail!("coverage FAIL: uncovered CR: {}", gate_cr.join(", "));
    }
    Ok(())
}

fn req_sort_key(r: &str) -> (u8, u32) {
    let (kind, n) = r.split_once('-').unwrap_or((r, "0"));
    let k = match kind {
        "FR" => 0,
        "CR" => 1,
        "INV" => 2,
        _ => 9,
    };
    (k, n.parse().unwrap_or(0))
}

fn yaml_scalar(text: &str, key: &str) -> Option<String> {
    let prefix = format!("{key}:");
    for line in text.lines() {
        let t = line.trim();
        if let Some(rest) = t.strip_prefix(&prefix) {
            let v = rest.trim().trim_matches('"');
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

/// Read a YAML sequence, accepting both block and inline (flow) form.
///
/// Supporting only block form made this silently return nothing for
/// `covers: [FR-1, CR-2]`, so scenarios appeared to cover nothing and the report
/// understated real coverage. A parser used for a gate must not fail quietly on
/// valid input.
fn yaml_seq(text: &str, key: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut in_list = false;
    let header = format!("{key}:");
    for line in text.lines() {
        let trimmed = line.trim();

        // Inline form: `key: [a, b, c]`
        if let Some(rest) = trimmed.strip_prefix(&header) {
            let rest = rest.trim();
            if let Some(inner) = rest.strip_prefix('[').and_then(|r| r.strip_suffix(']')) {
                for item in inner.split(',') {
                    let item = item.trim().trim_matches('"');
                    if !item.is_empty() {
                        out.push(item.to_string());
                    }
                }
                continue;
            }
            if rest.is_empty() {
                in_list = true;
                continue;
            }
        }

        // Block form:
        //   key:
        //     - a
        //     - b
        if in_list {
            if let Some(item) = trimmed.strip_prefix("- ") {
                out.push(item.trim().trim_matches('"').to_string());
            } else if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            } else {
                break;
            }
        }
    }
    out
}

/// Collect dependency names from a Cargo.toml, ignoring comments.
///
/// Matching raw file text would flag a crate for merely *explaining* in a
/// comment why it does not depend on something — which is exactly what happened
/// the first time this gate was tightened.
/// Dependencies declared in one specific section.
///
/// [`declared_dependencies`] deliberately matches every `*dependencies*` table,
/// which is right for "must never appear anywhere" rules. It is wrong for rules
/// about production dependencies only — a test-only `tokio` would trip them.
fn declared_dependencies_in_section(text: &str, section: &str) -> Vec<String> {
    let mut deps = Vec::new();
    let mut in_section = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('#') || trimmed.is_empty() {
            continue;
        }
        if trimmed.starts_with('[') {
            in_section = trimmed == section;
            continue;
        }
        if in_section {
            if let Some((name, _)) = trimmed.split_once('=') {
                deps.push(name.trim().trim_matches('"').to_string());
            }
        }
    }
    deps
}

fn declared_dependencies(text: &str) -> Vec<String> {
    let mut deps = Vec::new();
    let mut in_deps = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('#') || trimmed.is_empty() {
            continue;
        }
        if trimmed.starts_with('[') {
            in_deps = trimmed.contains("dependencies");
            continue;
        }
        if in_deps {
            if let Some((name, _)) = trimmed.split_once('=') {
                deps.push(name.trim().trim_matches('"').to_string());
            }
        }
    }
    deps
}

/// Enforce that the domain crate depends on no adapter and no ingress crate
/// (D14).
///
/// **This gate is only as good as the names below.** Because it works by name
/// matching, a rename that misses this function turns the check into a no-op that
/// passes forever without reporting anything — hence the self-check at the end.
fn check_deps() -> Result<()> {
    const FORBIDDEN_IN_CORE: &[&str] = &[
        "mock-server",
        "nova-responses-gateway",
        "harness",
        "conformance",
    ];

    let core = std::fs::read_to_string("crates/responses/Cargo.toml")?;
    let core_deps = declared_dependencies(&core);
    for forbidden in FORBIDDEN_IN_CORE {
        if core_deps.iter().any(|d| d == forbidden) {
            bail!("nova-responses must not depend on `{forbidden}`");
        }
    }

    // Guard against the gate silently rotting: the crate it protects must still
    // carry the name assumed above. Without this, a rename would make the loop
    // pass vacuously.
    if !core.contains("name = \"nova-responses\"") {
        bail!(
            "check-deps is out of date: crates/responses is no longer `nova-responses`, \
             so the forbidden-dependency list above may no longer match reality"
        );
    }

    // The orchestrator crate stays IO-free: it talks to core ports only, so the
    // whole claim/stream/commit path can be tested without a socket or a model.
    let agent = std::fs::read_to_string("crates/agent-runtime/Cargo.toml")?;
    for forbidden in ["reqwest", "hyper", "axum"] {
        if declared_dependencies_in_section(&agent, "[dependencies]")
            .iter()
            .any(|d| d == forbidden)
        {
            bail!(
                "nova-agent-runtime must not depend on `{forbidden}`: the orchestrator is \
                 kept IO-free so the whole claim/stream/commit path can be tested without \
                 a socket"
            );
        }
    }

    check_execution_claims_globally_through_the_port()?;
    check_service_and_gateway_boundaries()?;
    check_sdk_compat_is_python_only()?;
    check_coverage_baseline_tracks_invariants()?;
    let spec_covers = check_protocol_spec_is_publishable()?;

    println!("check-deps OK ({} gated requirement(s))", spec_covers.len());
    Ok(())
}

/// L4 stays a *compatibility harness*, not a second Rust implementation.
///
/// The SDK-compat layer exists to prove an unmodified official client works. A
/// `Cargo.toml` here would be a temptation to reimplement what the Python script
/// already exercises, inside the same workspace — and the two copies would drift.
/// A dependency on anything beyond `openai` would turn "compatible with the
/// official client" into "compatible with a pile of our own code".
fn check_sdk_compat_is_python_only() -> Result<()> {
    let dir = "verify/sdk-compat";
    if Path::new(&format!("{dir}/Cargo.toml")).exists() {
        bail!(
            "verify/sdk-compat must stay a pure-Python harness: a Cargo.toml here \
             would let the SDK-compat claim silently drift into a second Rust client"
        );
    }
    let req = std::fs::read_to_string(format!("{dir}/requirements.txt"))?;
    // A specifier is `name<op>version`; strip the operator and version, keeping
    // the bare distribution name. `openai>=1.60` → `openai`.
    let deps: Vec<&str> = req
        .lines()
        .filter(|l| !l.trim().is_empty() && !l.trim_start().starts_with('#'))
        .map(|l| {
            let l = l.trim();
            l.find(['=', '<', '>', '~', '!', ';'])
                .map(|i| &l[..i])
                .unwrap_or(l)
        })
        .collect();
    for dep in deps {
        if dep != "openai" {
            bail!(
                "verify/sdk-compat must depend only on the official `openai` SDK; \
                 found `{dep}`. Anything else would make the claim \"official client \
                 works unmodified\" untestable."
            );
        }
    }
    Ok(())
}

/// Every invariant in `docs/architecture/invariants.md` must appear in the
/// coverage baseline. Without this, a newly added invariant silently falls
/// outside the coverage gate — the gate only ever checks ids it already knows.
///
/// `INV-D11` is excluded here: it is the storage/separation rule, guarded
/// structurally by [`check_inflight_separated_from_store`] rather than by a
/// runtime contract.
fn check_coverage_baseline_tracks_invariants() -> Result<()> {
    let text = std::fs::read_to_string("docs/architecture/invariants.md")
        .context("docs/architecture/invariants.md is the invariant authority")?;
    let baseline = coverage_baseline();

    // Only table rows of the form `| **INV-xx** | ...` are live invariants. The
    // superseded ids (INV-3/10/13/14/15/53 …) appear only in prose like
    // "`INV-10 / … 已废止`" or "`~~INV-53~~`", which a plain `INV-` scan would
    // mistake for live ones.
    let mut ids: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for line in text.lines() {
        let Some(rest) = line.trim().strip_prefix("| **INV-") else {
            continue;
        };
        let id: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric())
            .collect();
        if id.is_empty() {
            continue;
        }
        ids.insert(format!("INV-{id}"));
    }

    if ids.is_empty() {
        bail!(
            "parsed no `INV-*` ids out of invariants.md; the parser and the document \
             have diverged, so this gate is checking nothing"
        );
    }

    let missing: Vec<&String> = ids
        .iter()
        .filter(|id| id.as_str() != "INV-D11" && !baseline.contains(id.as_str()))
        .collect();
    if !missing.is_empty() {
        bail!(
            "invariants.md declares {} which is absent from the coverage baseline. \
             Add it to `coverage_baseline()` and to the contract case / oracle that \
             substantiates it, or the invariant silently escapes the coverage gate.",
            missing.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")
        );
    }
    Ok(())
}

/// D25: execution claims through the ledger port, never over HTTP; claim is global.
///
/// Two things are guarded, and they are not the same thing.
///
/// 1. **No `/v1/agent/*` pull surface.** Execution is not a protocol: `nova-agentd`
///    reaches the ledger through `ResponseLedger`. An HTTP pull endpoint would add
///    a hop, a second authorisation path, and a second place for the attempt fence
///    to be checked — the arrangement whose failure mode (increments landing in one
///    process while subscribers were routed to another, with no error on any path)
///    cost D23 its rewrite.
/// 2. **Claim stays global.** The in-flight buffer is shared, so any execution
///    process may serve any queued response. A node filter would strand work on
///    nodes that happen to have no agent attached.
fn check_execution_claims_globally_through_the_port() -> Result<()> {
    let routes = std::fs::read_to_string("gateway/src/routes/mod.rs")?;
    if routes.contains("/v1/agent/") && routes.contains(".route(\"/v1/agent/") {
        bail!(
            "an /v1/agent/* route is registered again. Execution is not a protocol (D25): \
             nova-agentd claims through the ResponseLedger port. An HTTP pull surface adds \
             a hop, a second authorisation path and a second fence check, and it is how \
             increments once landed in a process no subscriber was reading."
        );
    }

    // The ledger's claim must stay global (D25). The in-flight buffer is shared, so
    // any execution process may claim any queued response; a node filter would
    // silently strand work on other nodes.
    let ledger_port = std::fs::read_to_string("crates/responses/src/ports/ledger.rs")?;
    if ledger_port.contains("node: &NodeTag") {
        bail!(
            "ResponseLedger::claim still takes a NodeTag. Claiming must be global (D25): \
             the in-flight buffer is shared, so a node filter would strand queued \
             responses on other nodes."
        );
    }

    Ok(())
}

/// `nova-responses` is the adapter-free service layer. It must not depend on any
/// concrete adapter — it talks to `nova-responses` ports only, so the
/// adapter choice is made by the assembler (the gateway), never by the library.
fn check_service_and_gateway_boundaries() -> Result<()> {
    let service = std::fs::read_to_string("crates/responses/Cargo.toml")?;
    let service_deps = declared_dependencies_in_section(&service, "[dependencies]");
    for forbidden in [
        "mock-server",
        "nova-agent-runtime",
        "mock-agentd",
    ] {
        if service_deps.iter().any(|d| d == forbidden) {
            bail!(
                "nova-responses must not depend on `{forbidden}`: the service layer talks \
                 to ports only, and the adapter choice belongs to the assembler"
            );
        }
    }

    Ok(())
}

/// FR-23: the protocol subset must ship as a publishable spec that names the
/// upstream revision it tracks.
///
/// Checked mechanically because the failure is silent: the code enforces a subset
/// either way, and a spec that has drifted still *reads* as authoritative. An
/// integrator following a stale document gets 400s that the document says are
/// impossible.
/// Field names on the deliberate-rejection list, parsed from
/// `EXPLICITLY_UNSUPPORTED_FIELDS`.
///
/// Each entry spans several lines, with the field name as the first string
/// literal after the opening paren; the second literal is the remedy text and
/// must not be mistaken for a name.
fn explicitly_unsupported_fields(protocol_mod: &str) -> Result<Vec<String>> {
    let start = protocol_mod
        .find("EXPLICITLY_UNSUPPORTED_FIELDS")
        .context("EXPLICITLY_UNSUPPORTED_FIELDS is the source of truth for the rejection list")?;
    let body = &protocol_mod[start..];
    let end = body
        .find("];")
        .context("EXPLICITLY_UNSUPPORTED_FIELDS is not terminated")?;

    let mut names = Vec::new();
    let mut expect_name = false;
    for line in body[..end].lines() {
        let line = line.trim();
        if line == "(" {
            expect_name = true;
            continue;
        }
        if expect_name {
            if let Some(rest) = line.strip_prefix('"') {
                if let Some(name) = rest.split('"').next() {
                    names.push(name.to_string());
                }
                expect_name = false;
            }
        }
    }
    if names.is_empty() {
        bail!(
            "parsed no field names out of EXPLICITLY_UNSUPPORTED_FIELDS; the parser \
             and the constant's formatting have diverged, so this gate is silently \
             checking nothing"
        );
    }
    Ok(names)
}

fn check_protocol_spec_is_publishable() -> Result<&'static [&'static str]> {
    let path = Path::new("docs/design/06-protocol-subset.md");
    let spec = std::fs::read_to_string(path)
        .with_context(|| format!("{} is the deliverable for FR-23", path.display()))?;

    // Must state which upstream revision it was derived from, otherwise "aligned
    // with the official protocol" is unfalsifiable.
    if !spec.contains("openai-openapi") {
        bail!(
            "{} must name the upstream specification it tracks (FR-23); without a \
             revision, a reader cannot tell whether the subset is current",
            path.display()
        );
    }
    let has_revision = spec
        .lines()
        .any(|l| l.contains("修订") || l.to_ascii_lowercase().contains("revision"));
    if !has_revision {
        bail!(
            "{} must pin an upstream revision date (FR-23)",
            path.display()
        );
    }

    // Every parameter the code rejects by name must appear in the document, so the
    // published rejection list cannot fall behind the enforced one.
    //
    // The list is read out of the source rather than restated here. Restating it
    // means two lists, and two lists disagree the first time one of them changes:
    // when `conversation` moved into the subset (D27), a hard-coded copy would
    // have kept demanding the document still call it rejected.
    let protocol_mod = std::fs::read_to_string("crates/responses/src/protocol/mod.rs")?;
    let rejected = explicitly_unsupported_fields(&protocol_mod)?;
    for param in &rejected {
        if !spec.contains(param.as_str()) {
            bail!(
                "`{param}` is on EXPLICITLY_UNSUPPORTED_FIELDS but absent from {} \
                 (FR-23); integrators would hit an undocumented 400",
                path.display()
            );
        }
    }

    // The item types the code refuses must likewise be listed.
    for item in ["item_reference", "reasoning"] {
        if !spec.contains(item) {
            bail!(
                "item type `{item}` is outside the subset but not documented in {} \
                 (FR-23)",
                path.display()
            );
        }
    }

    Ok(&["FR-23", "FR-27"])
}
