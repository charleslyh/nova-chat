use async_trait::async_trait;
use crate::{AgentId, Attempt, IdempotencyKey, SessionId, TurnId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionLock {
    Idle,
    Busy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnStatus {
    Pending,
    Claimed,
    Done,
    Failed,
}

#[derive(Debug, Clone)]
pub struct TurnRecord {
    pub turn_id: TurnId,
    pub session_id: SessionId,
    pub text: String,
    pub status: TurnStatus,
    pub attempt: Attempt,
    pub owner: Option<AgentId>,
    pub exec_deadline_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ClaimedTurn {
    pub turn: TurnRecord,
    pub attempt: Attempt,
    pub exec_deadline_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitOutcome {
    Accepted { turn_id: TurnId },
    Duplicate { turn_id: TurnId },
    Busy,
    /// INV-32: read-only degrade rejects new writes.
    ReadOnly,
}

#[derive(Debug, Error)]
pub enum MetaError {
    #[error("not found")]
    NotFound,
    #[error("stale attempt")]
    StaleAttempt,
    #[error("busy")]
    Busy,
    #[error("read only")]
    ReadOnly,
    #[error("internal: {0}")]
    Internal(String),
}

/// Minimal session/turn ledger (simulated buss-db). No capacity matching.
#[async_trait]
pub trait MetaStore: Send + Sync {
    async fn create_session(&self) -> Result<SessionId, MetaError>;

    async fn submit_turn(
        &self,
        session_id: SessionId,
        text: String,
        idempotency_key: IdempotencyKey,
        now_ms: u64,
    ) -> Result<SubmitOutcome, MetaError>;

    async fn claim_turn(
        &self,
        agent_id: AgentId,
        now_ms: u64,
        exec_ttl_ms: u64,
    ) -> Result<Option<ClaimedTurn>, MetaError>;

    async fn complete_turn(
        &self,
        turn_id: TurnId,
        expected_attempt: Attempt,
        to: TurnStatus,
    ) -> Result<(), MetaError>;

    async fn heartbeat(&self, agent_id: AgentId, now_ms: u64) -> Result<(), MetaError>;

    /// Reap timed-out claims; returns aborted (turn_id, old_attempt, session_id) for stream writes.
    async fn reap(&self, now_ms: u64, heartbeat_ttl_ms: u64) -> Result<Vec<(TurnId, Attempt, SessionId)>, MetaError>;

    async fn get_turn(&self, turn_id: TurnId) -> Result<Option<TurnRecord>, MetaError>;

    async fn lock(&self, session_id: SessionId) -> Result<SessionLock, MetaError>;

    /// Validate that attempt is still current for append fence.
    async fn check_attempt(&self, turn_id: TurnId, attempt: Attempt) -> Result<(), MetaError>;
}
