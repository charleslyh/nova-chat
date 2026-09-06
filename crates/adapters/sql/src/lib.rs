//! PostgreSQL adapters: the production carrier for the context store and the
//! ledger, and the backend used by L3 end-to-end scenarios.
//!
//! Why Postgres specifically: `WITH RECURSIVE` turns chain resolution into a
//! single query instead of one round trip per link (see `context.rs`). That is
//! the decisive property, not general SQL support.
//!
//! `sqlx`'s compile-time query macros (`query!`) are deliberately unused, so
//! building this crate never requires a reachable database — L0–L2 stay
//! infrastructure-free (D17). `migrate!` is fine: it only embeds local SQL.

mod context;
mod conversation;
mod error;
mod ledger;
mod row;
mod session;

pub use context::SqlContextStore;
pub use conversation::SqlConversationStore;
pub use error::SqlError;
pub use ledger::SqlResponseLedger;
pub use session::SqlSessionStore;

use std::sync::Arc;
use std::time::Duration;

use nova_responses_core::{ContentIntegrity, HmacSha256Integrity};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

/// Environment variable naming convention: configuration files carry the
/// **name** of the variable, never the connection string itself (SEC-4).
pub const DEFAULT_DATABASE_URL_ENV: &str = "NOVA_DATABASE_URL";

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Debug, Clone)]
pub struct SqlConfig {
    pub max_connections: u32,
    pub acquire_timeout: Duration,
    pub verify_integrity: bool,
}

impl Default for SqlConfig {
    fn default() -> Self {
        Self {
            max_connections: 16,
            // Bounded so an unreachable database surfaces as a prompt 503
            // rather than a hanging request (INV-46).
            acquire_timeout: Duration::from_secs(3),
            verify_integrity: true,
        }
    }
}

/// Assembled SQL backend.
pub struct SqlWorld {
    pub pool: PgPool,
    pub ledger: Arc<SqlResponseLedger>,
    pub context: Arc<SqlContextStore>,
    /// Conversation pointers (D27) and session state (D26). On the same pool, so
    /// nothing here introduces a fourth storage class (D21).
    pub conversation: Arc<SqlConversationStore>,
    pub session: Arc<SqlSessionStore>,
    pub integrity: Option<Arc<dyn ContentIntegrity>>,
}

impl SqlWorld {
    /// Connect using the connection string held in the named environment
    /// variable, run migrations, and assemble the adapters.
    pub async fn connect_from_env(
        url_env: &str,
        cfg: SqlConfig,
    ) -> Result<Self, SqlError> {
        let url = std::env::var(url_env).map_err(|_| {
            SqlError::Config(format!("environment variable {url_env} is not set"))
        })?;
        Self::connect(&url, cfg).await
    }

    pub async fn connect(url: &str, cfg: SqlConfig) -> Result<Self, SqlError> {
        let pool = PgPoolOptions::new()
            .max_connections(cfg.max_connections)
            .acquire_timeout(cfg.acquire_timeout)
            .connect(url)
            .await?;
        MIGRATOR.run(&pool).await?;

        let integrity: Option<Arc<dyn ContentIntegrity>> = if cfg.verify_integrity {
            // Startup fails when the key is absent, rather than storing records
            // that cannot be verified later (INV-44).
            Some(Arc::new(HmacSha256Integrity::from_env().map_err(|e| {
                SqlError::Config(format!("integrity key: {e}"))
            })?))
        } else {
            None
        };

        Ok(Self {
            ledger: Arc::new(SqlResponseLedger::new(pool.clone())),
            context: Arc::new(SqlContextStore::new(pool.clone(), integrity.clone())),
            conversation: Arc::new(SqlConversationStore::new(pool.clone())),
            session: Arc::new(SqlSessionStore::new(pool.clone())),
            integrity,
            pool,
        })
    }

    /// Remove all rows. Test helper only — never exposed through a port.
    ///
    /// `session_events` is truncated through `CASCADE` on `sessions`, but is
    /// named explicitly anyway: a truncate that silently depended on a foreign
    /// key would start leaving rows behind the day that key changed.
    pub async fn truncate_all(&self) -> Result<(), SqlError> {
        sqlx::query(
            "TRUNCATE responses, agent_heartbeats, session_events, sessions, conversations",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
