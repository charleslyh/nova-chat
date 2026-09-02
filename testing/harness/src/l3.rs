//! L3: end-to-end against the real carrier (PostgreSQL).
//!
//! Two roles:
//!
//! 1. **Reuse the L0 contract against the sql backend.** This is the acceptance
//!    criterion for the ports being genuine abstractions — the same assertions,
//!    a different adapter, no changes.
//! 2. **Verify what only a shared, durable store can show**: direct access from
//!    any node, chain affinity retired, and history surviving a restart.
//!
//! **Skipped, not failed, when no database is configured** (D17): L0–L2 must
//! stay runnable on a machine with no infrastructure.

use std::path::Path;

use anyhow::{Context, Result};
use conformance::PortSet;
use nova_responses_core::NodeTag;

/// Environment variable naming the L3 database. Absent ⇒ skip.
pub const DATABASE_URL_ENV: &str = "NOVA_TEST_DATABASE_URL";

/// Why the event-log contract case is not evidence about the sql backend.
///
/// Stated as a constant and printed during every L3 run so the limitation
/// travels with the output rather than living only in a comment.
pub const EVENT_LOG_IS_NOT_SQL_BACKED: &str =
    "in-flight events are deliberately never persisted (D21), so no sql event log exists to verify";
const INTEGRITY_KEY_ENV: &str = "NOVA_INTEGRITY_KEY";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum L3Availability {
    Ready,
    Skipped { reason: String },
}

/// Build a [`PortSet`] over the sql adapters, or explain why not.
///
/// The in-flight event log stays in process memory even here: that is the
/// deliberate tiering decision (D21), not something L3 is meant to replace.
pub async fn sql_ports_from_env() -> Result<(Option<PortSet>, L3Availability)> {
    let Ok(url) = std::env::var(DATABASE_URL_ENV) else {
        return Ok((
            None,
            L3Availability::Skipped {
                reason: format!("{DATABASE_URL_ENV} is not set"),
            },
        ));
    };

    if std::env::var(INTEGRITY_KEY_ENV).is_err() {
        // Matches production behaviour: verification enabled without a key is a
        // startup failure, never a silent downgrade (INV-44).
        std::env::set_var(INTEGRITY_KEY_ENV, "l3-harness-key-0123456789abcdef");
    }

    let world = adapters_sql::SqlWorld::connect(&url, adapters_sql::SqlConfig::default())
        .await
        .with_context(|| format!("connecting to {DATABASE_URL_ENV}"))?;
    world.truncate_all().await.context("truncating L3 tables")?;

    // The in-flight event log stays in process memory even here: that is the
    // deliberate tiering decision (D21), not a gap L3 is meant to close.
    //
    // Consequence worth stating plainly: the `event-log` contract case therefore
    // runs against the *mem* implementation during L3 as well. There is no sql
    // event log to verify, and presenting this run as though there were would
    // misrepresent what was checked — see `EVENT_LOG_IS_NOT_SQL_BACKED`.
    let mem = adapters_mem::MemWorld::new();
    let ports = PortSet {
        ledger: world.ledger.clone(),
        event_log: mem.event_log.clone(),
        context: world.context.clone(),
        integrity: world.integrity.clone(),
        node_tag: NodeTag::parse("node-a").expect("static tag"),
    };
    Ok((Some(ports), L3Availability::Ready))
}

/// Run the L0 contract plus the sql-specific scenarios.
pub async fn run_l3_dir(dir: &Path) -> Result<Vec<String>> {
    let (ports, availability) = sql_ports_from_env().await?;
    let Some(ports) = ports else {
        if let L3Availability::Skipped { reason } = availability {
            eprintln!("  l3 skipped: {reason}");
        }
        return Ok(vec![]);
    };

    let mut names = Vec::new();

    // 1. The shared contract, unchanged, against the real carrier.
    let report = conformance::run_suite_reported(&ports, "sql").await;
    if !report.skipped.is_empty() {
        // A backend that cannot supply a port must not have that fact absorbed
        // into a passing line of output.
        for (case, reason) in &report.skipped {
            eprintln!("  [sql] case `{case}` skipped: {reason}");
        }
    }
    names.push("sql-port-contract".to_string());
    eprintln!(
        "  [sql] note: the `event-log` case ran against the in-memory log — {}",
        EVENT_LOG_IS_NOT_SQL_BACKED
    );

    // 2. Properties that only appear with shared, durable storage.
    names.extend(run_shared_store_checks(&ports).await?);

    // 3. Declarative scenarios placed in the L3 directory.
    //
    // These must either execute or fail loudly. The previous version printed a
    // note and moved on, so a scenario dropped in here would be silently ignored
    // forever while the level still reported OK — the worst possible outcome for
    // a directory whose whole purpose is to be executed.
    if dir.exists() {
        let mut paths: Vec<_> = std::fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("yaml"))
            .filter(|p| {
                std::fs::read_to_string(p)
                    .map(|text| {
                        !text
                            .lines()
                            .map(str::trim)
                            .all(|line| line.is_empty() || line.starts_with('#'))
                    })
                    .unwrap_or(false)
            })
            .collect();
        paths.sort();
        if !paths.is_empty() {
            anyhow::bail!(
                "found {} declarative scenario(s) under {} but the L3 runner cannot execute \
                 them: the L1 step vocabulary is bound to the in-process MemWorld, so running \
                 them here would silently verify the wrong backend. Either express the check \
                 in `run_shared_store_checks` (Rust, against the sql PortSet) or extend the \
                 runner deliberately. Files: {}",
                paths.len(),
                dir.display(),
                paths
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }

    Ok(names)
}

async fn run_shared_store_checks(ports: &PortSet) -> Result<Vec<String>> {
    use nova_responses_core::{
        Attempt, ChainLimits, IdempotencyKey, ResponseId, ResponseItem, ResponseStatus,
        StoredResponse, TenantId, Usage,
    };

    let mut names = Vec::new();
    let tenant = TenantId::parse("l3-tenant").expect("tenant");

    // --- sql-shared-store-no-forward -------------------------------------
    eprint!("  sql-shared-store-no-forward ... ");
    assert!(
        ports.context.is_shared(),
        "the sql adapter must report shared storage, otherwise the ingress layer \
         keeps forwarding content reads and chain affinity never retires"
    );
    eprintln!("ok");
    names.push("sql-shared-store-no-forward".into());

    // --- sql-multi-turn-chain --------------------------------------------
    eprint!("  sql-multi-turn-chain ... ");
    let mut previous: Option<ResponseId> = None;
    let mut ids = Vec::new();
    for turn in 0..5 {
        // Note the node tag varies: with shared storage a chain may span nodes,
        // which is exactly what chain affinity existed to prevent.
        let tag = NodeTag::parse(if turn % 2 == 0 { "node-a" } else { "node-b" }).expect("tag");
        let id = ResponseId::new(tag.clone());
        let record = StoredResponse {
            response_id: id.clone(),
            previous_response_id: previous.clone(),
            tenant_id: tenant.clone(),
            model: "m".into(),
            instructions: Some("L3-INSTRUCTIONS".into()),
            input_items: vec![ResponseItem::user_text(format!("q{turn}"))],
            output_items: vec![ResponseItem::assistant_text(format!("a{turn}"))],
            status: ResponseStatus::Completed,
            usage: Usage::new(1, 1),
            created_at_ms: 1_000 + turn,
            completed_at_ms: Some(2_000 + turn),
            stored: true,
            expires_at_ms: None,
            integrity: None,
            integrity_alg: None,
            node_tag: tag,
            idempotency_key: None,
            owner: None,
            attempt: Attempt::default(),
        };
        ports.context.put(record).await.context("put")?;
        previous = Some(id.clone());
        ids.push(id);
    }

    let resolved = ports
        .context
        .resolve_chain(&tenant, ids.last().unwrap(), ChainLimits::default())
        .await
        .context("recursive chain resolution")?;
    assert_eq!(resolved.depth, 5, "recursive walk must return every link");
    assert_eq!(resolved.items.len(), 10);
    let encoded = nova_responses_core::canonical_items(&resolved.items);
    assert!(
        !encoded.contains("L3-INSTRUCTIONS"),
        "the recursive query must not select the instructions column (INV-49)"
    );
    // Chronological order.
    assert!(
        nova_responses_core::canonical_items(&resolved.items[..1]).contains("q0"),
        "oldest link must come first"
    );
    eprintln!("ok");
    names.push("sql-multi-turn-chain".into());

    // --- sql-restart-history-intact --------------------------------------
    eprint!("  sql-restart-history-intact ... ");
    // Simulate a restart: reclaim this node's in-flight work, then confirm the
    // completed history is untouched. This is the failure semantics tiering
    // promises — in-flight fails explicitly, history survives (FR-38).
    let in_flight_id = ResponseId::new(ports.node_tag.clone());
    let in_flight = StoredResponse {
        response_id: in_flight_id.clone(),
        previous_response_id: None,
        tenant_id: tenant.clone(),
        model: "m".into(),
        instructions: None,
        input_items: vec![ResponseItem::user_text("pending")],
        output_items: vec![],
        status: ResponseStatus::Queued,
        usage: Usage::default(),
        created_at_ms: 3_000,
        completed_at_ms: None,
        stored: true,
        expires_at_ms: None,
        integrity: None,
        integrity_alg: None,
        node_tag: ports.node_tag.clone(),
        idempotency_key: None,
        owner: None,
        attempt: Attempt::default(),
    };
    ports
        .ledger
        .create(
            in_flight,
            IdempotencyKey(uuid::Uuid::new_v4().to_string()),
            3_000,
        )
        .await
        .context("create in-flight")?;

    ports
        .ledger
        .reclaim_orphans(&ports.node_tag, 4_000)
        .await
        .context("reclaim")?;

    let failed = ports
        .ledger
        .get(&in_flight_id)
        .await?
        .expect("record present");
    assert_eq!(
        failed.status,
        ResponseStatus::Failed,
        "in-flight work must fail explicitly after a restart"
    );

    let history = ports
        .context
        .resolve_chain(&tenant, ids.last().unwrap(), ChainLimits::default())
        .await
        .context("history after restart")?;
    assert_eq!(
        history.depth, 5,
        "completed history must survive a restart untouched"
    );
    eprintln!("ok");
    names.push("sql-restart-history-intact".into());

    // --- sql-expiry-sweep ------------------------------------------------
    eprint!("  sql-expiry-sweep ... ");
    let expiring = ResponseId::new(ports.node_tag.clone());
    let mut rec = StoredResponse {
        response_id: expiring.clone(),
        previous_response_id: None,
        tenant_id: tenant.clone(),
        model: "m".into(),
        instructions: None,
        input_items: vec![ResponseItem::user_text("temp")],
        output_items: vec![],
        status: ResponseStatus::Completed,
        usage: Usage::default(),
        created_at_ms: 1_000,
        completed_at_ms: Some(1_100),
        stored: true,
        expires_at_ms: Some(9_000),
        integrity: None,
        integrity_alg: None,
        node_tag: ports.node_tag.clone(),
        idempotency_key: None,
        owner: None,
        attempt: Attempt::default(),
    };
    ports.context.put(rec.clone()).await?;
    assert_eq!(ports.context.sweep_expired(8_999, 100).await?, 0);
    assert!(ports.context.get(&tenant, &expiring).await?.is_some());
    assert_eq!(ports.context.sweep_expired(9_000, 100).await?, 1);
    assert!(ports.context.get(&tenant, &expiring).await?.is_none());
    rec.expires_at_ms = None;
    eprintln!("ok");
    names.push("sql-expiry-sweep".into());

    // --- sql-tenant-purge ------------------------------------------------
    eprint!("  sql-tenant-purge ... ");
    let other = TenantId::parse("l3-other").expect("tenant");
    let keep = ResponseId::new(ports.node_tag.clone());
    ports
        .context
        .put(StoredResponse {
            response_id: keep.clone(),
            previous_response_id: None,
            tenant_id: other.clone(),
            model: "m".into(),
            instructions: None,
            input_items: vec![ResponseItem::user_text("keep")],
            output_items: vec![],
            status: ResponseStatus::Completed,
            usage: Usage::default(),
            created_at_ms: 1_000,
            completed_at_ms: Some(1_100),
            stored: true,
            expires_at_ms: None,
            integrity: None,
            integrity_alg: None,
            node_tag: ports.node_tag.clone(),
            idempotency_key: None,
            owner: None,
            attempt: Attempt::default(),
        })
        .await?;

    let purged = ports.context.delete_by_tenant(&tenant).await?;
    assert!(purged >= 5, "expected the whole tenant to be cleared, got {purged}");
    assert!(
        ports.context.get(&other, &keep).await?.is_some(),
        "purge must be scoped to one tenant"
    );
    eprintln!("ok");
    names.push("sql-tenant-purge".into());

    Ok(names)
}
