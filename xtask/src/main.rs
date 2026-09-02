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
    Deploy {
        action: String,
    },
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
        Cmd::Deploy { action } => deploy(&action)?,
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
            let _names = harness::run_l1_dir(Path::new("testing/scenarios/l1")).await?;
            println!("verify l1 OK");
        }
        "l2" => {
            println!("verify l2");
            eprint!("  procs up ... ");
            procs("up").await?;
            eprintln!("ok");
            let _names = harness::run_l2_dir(Path::new("testing/scenarios/l2")).await?;
            eprint!("  procs down ... ");
            procs("down").await?;
            eprintln!("ok");
            println!("verify l2 OK");
        }
        "l3" => {
            println!("verify l3");
            // Skipped rather than failed when no database is configured, so a
            // machine without infrastructure can still run everything else (D17).
            let names = harness::run_l3_dir(Path::new("testing/scenarios/l3")).await?;
            if names.is_empty() {
                println!("verify l3 SKIPPED (no database configured)");
            } else {
                println!("verify l3 OK ({} checks)", names.len());
            }
        }
        other => bail!("unknown level {other}"),
    }
    Ok(())
}

async fn procs(action: &str) -> Result<()> {
    let run_dir = PathBuf::from("target/nova-procs");
    std::fs::create_dir_all(&run_dir)?;
    match action {
        "up" => {
            procs_down(&run_dir)?;
            // A leftover sim or gateway may still hold these ports; clear them
            // before starting the L2 fixture.
            kill_listeners(&[18080, 18081, 18082])?;
            // Three peer nodes: every node can create, and every node runs its
            // own sweeper. There is no authority node any more.
            for (tag, port) in [("node-a", 18080), ("node-b", 18081), ("node-c", 18082)] {
                start_bin(
                    "nova-responses-gateway",
                    &["--config", &format!("testing/config/{tag}.toml")],
                    run_dir.join(format!("{tag}.pid")),
                )?;
                let _ = port;
            }
            // No execution process to start: each node runs its own engine (D23).
            wait_port("127.0.0.1:18080", Duration::from_secs(20)).await?;
            wait_port("127.0.0.1:18081", Duration::from_secs(20)).await?;
            wait_port("127.0.0.1:18082", Duration::from_secs(20)).await?;
        }
        "down" => {
            procs_down(&run_dir)?;
            kill_listeners(&[18080, 18081, 18082])?;
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
        "agent.pid",
        "worker.pid",
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
    // Required whenever peers are configured: without it, node-to-node forwards
    // cannot be authenticated and the internal tenant header would have to be
    // trusted blindly.
    ("NOVA_INTERNAL_TOKEN", "l2-fixture-internal-token"),
    ("NOVA_INTEGRITY_KEY", "l2-fixture-integrity-key-0123456789"),
];

fn start_bin(bin: &str, args: &[&str], pidfile: PathBuf) -> Result<()> {
    let status = Command::new("cargo")
        .args(["build", "-p", bin])
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

async fn coverage() -> Result<()> {
    use std::collections::{BTreeMap, BTreeSet};

    // Baseline follows `docs/requirements/spec.md` v3. Keep in step with that
    // document: an id here that no longer exists there would silently demand
    // coverage for a requirement that was withdrawn.
    let baseline: BTreeSet<&str> = [
        // Lifecycle.
        "FR-1", "FR-2", "FR-3", "FR-4", "FR-5", "FR-6", "FR-7", "FR-8",
        // Streaming and resumption.
        "FR-9", "FR-10", "FR-11", "FR-12", "FR-13", "FR-14",
        // Storage and context.
        "FR-15", "FR-16", "FR-17", "FR-18", "FR-19", "FR-20", "FR-21", "FR-22",
        // Protocol subset.
        "FR-23", "FR-24", "FR-25", "FR-26", "FR-27", "FR-28",
        // Ingress and routing.
        "FR-29", "FR-30", "FR-31", "FR-32", "FR-33",
        // Reliability.
        "FR-34", "FR-35", "FR-36", "FR-37", "FR-38", "FR-39",
        // Correctness.
        "CR-1", "CR-2", "CR-3", "CR-4", "CR-5", "CR-6", "CR-7", "CR-8", "CR-9", "CR-10",
        "CR-11", "CR-12", "CR-13",
        // Invariants still in force.
        "INV-1", "INV-2", "INV-5", "INV-6", "INV-11", "INV-12", "INV-16", "INV-29", "INV-30",
        "INV-32", "INV-33", "INV-34", "INV-35", "INV-40", "INV-41", "INV-42", "INV-43",
        "INV-44", "INV-45", "INV-46", "INV-47", "INV-49", "INV-50", "INV-51", "INV-52",
        // Security.
        "SEC-2", "SEC-3", "SEC-5", "SEC-6", "SEC-7",
    ]
    .into_iter()
    .collect();

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
    let mut l3 = 0usize;

    for (label, dir) in [
        ("l1", "testing/scenarios/l1"),
        ("l2", "testing/scenarios/l2"),
        ("l3", "testing/scenarios/l3"),
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
                _ => l3 += 1,
            }
        }
    }

    // Requirements whose verification is intentionally deferred. Listing them
    // here keeps them visible in the report instead of quietly missing.
    //
    // FR-13/FR-14/FR-30/FR-31/FR-32 and SEC-5 are HTTP-level routing and
    // ownership properties covered by the gateway's own contract tests rather
    // than by declarative scenarios; FR-34 (graceful drain) needs a real signal,
    // so it lives at L2.
    // Only FR-31 remains genuinely deferred: it asserts that a *shared* store
    // removes node-to-node content forwarding, which cannot be observed without a
    // real database. It is verified by L3 (`sql-shared-store-no-forward`) and is
    // therefore covered whenever a database is configured.
    //
    // FR-23 moved to `check-deps` (the published spec is gated mechanically),
    // FR-32 into the drain scenario, SEC-5 into cross-tenant-404-http, and INV-34
    // into the L0 durability-order case. Each had been listed here while actually
    // being verifiable — which is the failure mode this list is most prone to:
    // once an id is written down as deferred, nobody re-examines it.
    let deferred: BTreeSet<&str> = ["FR-31"].into_iter().collect();

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
    println!("  scenarios     {l1} L1 · {l2} L2 · {l3} L3");
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

    std::fs::create_dir_all("testing/reports")?;
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
    std::fs::write("testing/reports/traceability.md", &md)?;

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
        "adapters-mem",
        "adapters-sql",
        "nova-responses-gateway",
        "harness",
        "conformance",
    ];

    let core = std::fs::read_to_string("crates/core/Cargo.toml")?;
    let core_deps = declared_dependencies(&core);
    for forbidden in FORBIDDEN_IN_CORE {
        if core_deps.iter().any(|d| d == forbidden) {
            bail!("nova-responses-core must not depend on `{forbidden}`");
        }
    }

    // Guard against the gate silently rotting: the crate it protects must still
    // carry the name assumed above. Without this, a rename would make the loop
    // pass vacuously.
    if !core.contains("name = \"nova-responses-core\"") {
        bail!(
            "check-deps is out of date: crates/core is no longer `nova-responses-core`, \
             so the forbidden-dependency list above may no longer match reality"
        );
    }

    // The two adapters are alternatives, not layers.
    let sql_deps = declared_dependencies(&std::fs::read_to_string(
        "crates/adapters/sql/Cargo.toml",
    )?);
    if sql_deps.iter().any(|d| d == "adapters-mem") {
        bail!("adapters-sql must not depend on adapters-mem");
    }
    let mem_deps = declared_dependencies(&std::fs::read_to_string(
        "crates/adapters/mem/Cargo.toml",
    )?);
    if mem_deps.iter().any(|d| d == "adapters-sql") {
        bail!("adapters-mem must not depend on adapters-sql");
    }

    // L0 must stay buildable without a database driver (D17).
    let conformance_deps = declared_dependencies(&std::fs::read_to_string(
        "testing/conformance/Cargo.toml",
    )?);
    // The mock scheduler adapter must stay model-free and IO-free: its whole
    // purpose is to let integration tests run with no provider. An HTTP client
    // here would mean a test could silently start making real calls.
    let mock_sched = std::fs::read_to_string("crates/adapters/completions-mock/Cargo.toml")?;
    let mock_sched_deps = declared_dependencies(&mock_sched);
    for forbidden in ["reqwest", "hyper", "adapters-sql", "sqlx", "nova-agent"] {
        if mock_sched_deps.iter().any(|d| d == forbidden) {
            bail!(
                "adapters-completions-mock must not depend on `{forbidden}`: it exists so \
                 tests need neither a provider nor a database"
            );
        }
    }

    // The execution-side worker faces our gateway, not a provider. A dependency on
    // a concrete scheduler adapter would invert that: the loop would then know
    // which provider it serves, and swapping one would mean changing the loop.
    let agent = std::fs::read_to_string("crates/agent/Cargo.toml")?;
    for forbidden in ["reqwest", "hyper", "axum", "sqlx"] {
        if declared_dependencies_in_section(&agent, "[dependencies]")
            .iter()
            .any(|d| d == forbidden)
        {
            bail!(
                "nova-agent must not depend on `{forbidden}`: the work loop is kept IO-free so \
                 the whole claim/stream/submit path can be tested without a socket"
            );
        }
    }

    for forbidden in ["adapters-sql", "sqlx"] {
        if conformance_deps.iter().any(|d| d == forbidden) {
            bail!(
                "conformance must not depend on `{forbidden}`: L0 has to build without a \
                 database driver (D17). Feed the sql backend through `run_suite` from the \
                 L3 runner instead."
            );
        }
    }

    check_execution_is_internal()?;
    let spec_covers = check_protocol_spec_is_publishable()?;

    println!("check-deps OK ({} gated requirement(s))", spec_covers.len());
    Ok(())
}

/// D23: execution is in-process, never a protocol.
///
/// Guards against the pull endpoints returning. They were not merely redundant —
/// they allowed a worker attached to one node to claim another node's generation,
/// whose increments then landed in the wrong process heap while subscribers were
/// routed to the owning node and saw silence. Reintroducing them would reopen a
/// defect that produced no error on any path.
fn check_execution_is_internal() -> Result<()> {
    let routes = std::fs::read_to_string("crates/gateway/src/routes/mod.rs")?;
    if routes.contains("/v1/agent/") && routes.contains(".route(\"/v1/agent/") {
        bail!(
            "an /v1/agent/* route is registered again. Execution is in-process (D23): a \
             generation is run by the node that created it, because that node holds its \
             in-flight event buffer. A pull endpoint lets another node claim it, and the \
             resulting silence on the subscriber's stream raises no error anywhere."
        );
    }

    // The ledger's claim must stay node-scoped. Without the parameter the constraint
    // has nowhere to live, and a shared ledger hands work across nodes again.
    let ledger_port = std::fs::read_to_string("crates/core/src/ports/ledger.rs")?;
    if !ledger_port.contains("node: &NodeTag") {
        bail!(
            "ResponseLedger::claim no longer takes a NodeTag. Claiming must be scoped to \
             the owning node (FR-4 / D23); the in-memory backend used to make this hold \
             by accident, which is exactly why it needs to be explicit."
        );
    }

    // And the sql implementation must actually filter on it.
    //
    // Scoped to the claim statement, not the whole file: the first version of this
    // check searched the file and was satisfied by the explanatory comment that
    // mentions the predicate — so removing the predicate itself passed the gate. A
    // gate that its own documentation can satisfy checks nothing.
    let sql_ledger = std::fs::read_to_string("crates/adapters/sql/src/ledger.rs")?;
    // Two facts that cannot be satisfied by prose: the predicate as it appears in the
    // WHERE clause, and the bind that supplies it.
    let has_predicate = sql_ledger.contains("WHERE status = 'queued' AND node_tag = $3");
    let has_bind = sql_ledger.contains(".bind(node.as_str())");
    if !(has_predicate && has_bind) {
        bail!(
            "the sql claim statement no longer filters by node_tag. With a shared ledger \
             this hands node-b's generation to node-a, whose increments land in the wrong \
             process — subscribers see the created event and then nothing, with no error."
        );
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
    let request = std::fs::read_to_string("crates/core/src/protocol/request.rs")?;
    for param in ["conversation", "context_management", "prompt"] {
        if !request.contains(param) {
            bail!(
                "`{param}` is documented as rejected but no longer appears in \
                 request.rs; the published subset would overstate what is enforced"
            );
        }
        if !spec.contains(param) {
            bail!(
                "`{param}` is rejected by request.rs but absent from {} (FR-23); \
                 integrators would hit an undocumented 400",
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

fn deploy(action: &str) -> Result<()> {
    let compose = Path::new("deploy/docker/docker-compose.yml");
    if !compose.exists() {
        bail!("missing {}", compose.display());
    }
    let docker = Command::new("docker").arg("version").output();
    match docker {
        Ok(o) if o.status.success() => {
            let status = Command::new("docker")
                .args(["compose", "--project-directory", "deploy/docker", action])
                .status()?;
            if !status.success() {
                bail!("docker compose {action} failed");
            }
        }
        _ => {
            println!(
                "deploy skipped: Docker unavailable (D17). L0–L2 verification does not require it."
            );
        }
    }
    Ok(())
}
