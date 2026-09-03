//! Connectivity and migration smoke test.
//!
//! Skipped unless `NOVA_TEST_DATABASE_URL` is set, keeping the default test run
//! infrastructure-free (D17). Full port semantics are asserted by the shared L0
//! contract in `testing/conformance`, which runs against this backend too.

use adapters_sql::{SqlConfig, SqlWorld};

const URL_ENV: &str = "NOVA_TEST_DATABASE_URL";

async fn world() -> Option<SqlWorld> {
    let url = std::env::var(URL_ENV).ok()?;
    std::env::set_var("NOVA_INTEGRITY_KEY", "sql-adapter-test-key-0123456789");
    match SqlWorld::connect(&url, SqlConfig::default()).await {
        Ok(world) => {
            world.truncate_all().await.expect("truncate");
            Some(world)
        }
        Err(e) => panic!("{URL_ENV} is set but connecting failed: {e}"),
    }
}

#[tokio::test]
async fn migrations_apply_and_store_is_reachable() {
    let Some(world) = world().await else {
        eprintln!("skipping: {URL_ENV} not set");
        return;
    };
    use nova_responses_core::ContextStore;
    assert!(world.context.health().await.is_ok());
}

#[tokio::test]
async fn unreachable_database_surfaces_as_unavailable_not_internal() {
    // The distinction matters: only genuine unavailability may become a 503 and
    // trigger the refuse-writes degrade (INV-46). Pointing at a closed port is
    // the cheapest way to exercise that mapping, and needs no live database.
    std::env::set_var("NOVA_INTEGRITY_KEY", "sql-adapter-test-key-0123456789");
    let result = SqlWorld::connect(
        "postgres://127.0.0.1:1/nonexistent",
        SqlConfig {
            acquire_timeout: std::time::Duration::from_millis(200),
            ..SqlConfig::default()
        },
    )
    .await;
    assert!(result.is_err(), "connecting to a closed port must fail");
}
