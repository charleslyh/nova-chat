//! Session streaming API: POST session/turn + GET snapshot/SSE (D19).

use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Parser;
use futures::stream;
use adapters_mem::MemWorld;
use nova_sessions_core::{
    Attempt, Bubble, EventKind, IdempotencyKey, SessionId, SessionSnapshot, StreamEvent, TurnId,
};
use nova_sessions_core::{
    MetaStore, SnapshotStore, StreamChannel, StreamError, SubmitOutcome, TurnStatus,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "testing/config/home.toml")]
    config: PathBuf,
}

#[derive(Debug, Deserialize, Clone)]
struct Config {
    #[serde(default = "default_region")]
    region: String,
    /// home = authoritative; edge = forward writes to home_upstream
    role: String,
    listen: String,
    #[serde(default)]
    home_upstream: Option<String>,
    #[serde(default = "default_true")]
    run_reaper: bool,
    /// FR-18: max Pending+Claimed turns before Overloaded (home only).
    #[serde(default = "default_pending_limit")]
    pending_limit: usize,
}

fn default_region() -> String {
    "default".into()
}

fn default_true() -> bool {
    true
}

fn default_pending_limit() -> usize {
    10_000
}

#[derive(Clone)]
struct AppState {
    cfg: Config,
    world: MemWorld,
    http: reqwest::Client,
    /// Shared world only on home; edge uses upstream for writes/reads.
    _lock: Arc<Mutex<()>>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let text = std::fs::read_to_string(&args.config)
        .with_context(|| format!("config {}", args.config.display()))?;
    let cfg: Config = toml::from_str(&text)?;
    let addr: SocketAddr = cfg.listen.parse()?;

    let world = MemWorld::new();
    if cfg.role == "home" {
        world.meta.set_pending_limit(cfg.pending_limit);
    }
    let state = AppState {
        cfg: cfg.clone(),
        world: world.clone(),
        http: reqwest::Client::new(),
        _lock: Arc::new(Mutex::new(())),
    };

    if cfg.role == "home" && cfg.run_reaper {
        let w = world.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                if let Err(e) = reap_once(&w).await {
                    warn!(error = %e, "reaper");
                }
            }
        });
    }

    let app = Router::new()
        .route("/health", get(health))
        .route("/v1/sessions", post(create_session))
        .route("/v1/sessions/{id}/turns", post(create_turn))
        .route("/v1/sessions/{id}/snapshot", get(get_snapshot))
        .route("/v1/sessions/{id}/stream", get(stream_sse))
        .route("/v1/sessions/{id}/events", get(list_events))
        // ops / test: INV-32 read-only degrade toggle (home)
        .route("/v1/admin/read_only", post(set_read_only))
        .route("/v1/admin/pending_limit", post(set_pending_limit))
        .route("/v1/admin/trim_hot", post(trim_hot))
        // agent-facing (home only)
        .route("/v1/agent/claim", post(agent_claim))
        .route("/v1/agent/heartbeat", post(agent_heartbeat))
        .route("/v1/agent/append", post(agent_append))
        .route("/v1/agent/complete", post(agent_complete))
        .layer(CorsLayer::permissive())
        .with_state(state);

    if let Some(parent) = args.config.parent() {
        let marker = parent.join(format!(".ready-{}", cfg.region));
        let _ = std::fs::write(&marker, b"ok");
    }

    info!(region = %cfg.region, role = %cfg.role, %addr, "nova-sessions-gateway listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(st): State<AppState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "ok": true,
        "region": st.cfg.region,
        "role": st.cfg.role,
    }))
}

async fn reap_once(world: &MemWorld) -> Result<()> {
    let now = now_ms();
    let aborted = world.meta.reap(now, 90_000).await?;
    for (turn_id, _old_attempt, session_id) in aborted {
        // Envelope only (no attempt) — fence already raised in meta.
        let _ = append_and_project(
            world,
            StreamEvent {
                session_id,
                seq: 0,
                kind: EventKind::AttemptAborted,
                turn_id: Some(turn_id),
                attempt: None,
                payload: "reaped".into(),
            },
        )
        .await;
        let _ = append_and_project(
            world,
            StreamEvent {
                session_id,
                seq: 0,
                kind: EventKind::TurnFailed,
                turn_id: Some(turn_id),
                attempt: None,
                payload: "reaped".into(),
            },
        )
        .await;
        let _ = append_and_project(
            world,
            StreamEvent {
                session_id,
                seq: 0,
                kind: EventKind::SessionIdle,
                turn_id: None,
                attempt: None,
                payload: String::new(),
            },
        )
        .await;
        if let Ok(Some(mut snap)) = world.snapshot.get(session_id).await {
            snap.running.clear();
            let _ = world.snapshot.put(snap).await;
            world.sync_mirror(session_id).await;
        }
    }
    Ok(())
}

/// Authority append then project into in-process mirror (V12).
async fn append_and_project(
    world: &MemWorld,
    mut event: StreamEvent,
) -> Result<u64, StreamError> {
    let session_id = event.session_id;
    let seq = world.stream.append(event.clone()).await?;
    event.seq = seq;
    world.mirror.project_event(event);
    let _ = session_id;
    Ok(seq)
}

async fn project_snapshot_now(world: &MemWorld, session_id: SessionId) {
    if let Ok(Some(s)) = world.snapshot.get(session_id).await {
        let _ = world.mirror.project_snapshot(s);
    }
}

#[derive(Serialize)]
struct SessionCreated {
    session_id: SessionId,
}

async fn create_session(State(st): State<AppState>) -> Response {
    if st.cfg.role == "edge" {
        return forward_json(&st, "POST", "/v1/sessions", None::<()>).await;
    }
    match st.world.meta.create_session().await {
        Ok(id) => {
            let snap = SessionSnapshot {
                session_id: id,
                snapshot_seq: 0,
                bubbles: vec![],
                running: vec![],
            };
            let _ = st.world.snapshot.put(snap.clone()).await;
            let _ = st.world.mirror.project_snapshot(snap);
            (StatusCode::CREATED, Json(SessionCreated { session_id: id })).into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct TurnBody {
    text: String,
    #[serde(default)]
    idempotency_key: Option<String>,
    #[serde(default)]
    stream: bool,
}

#[derive(Serialize)]
struct TurnCreated {
    turn_id: TurnId,
}

async fn create_turn(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<TurnBody>,
) -> Response {
    if st.cfg.role == "edge" {
        return forward_json(&st, "POST", &format!("/v1/sessions/{id}/turns"), Some(&body)).await;
    }
    let Ok(session_id) = Uuid::parse_str(&id).map(SessionId) else {
        return err(StatusCode::BAD_REQUEST, "invalid session_id".into());
    };
    let key = IdempotencyKey(
        body.idempotency_key
            .unwrap_or_else(|| Uuid::new_v4().to_string()),
    );
    let outcome = match st
        .world
        .meta
        .submit_turn(session_id, body.text.clone(), key, now_ms())
        .await
    {
        Ok(o) => o,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };

    match outcome {
        SubmitOutcome::Busy => err(StatusCode::CONFLICT, "session busy".into()),
        SubmitOutcome::ReadOnly => err(StatusCode::SERVICE_UNAVAILABLE, "read_only".into()),
        SubmitOutcome::Overloaded => {
            err(StatusCode::TOO_MANY_REQUESTS, "overloaded".into())
        }
        SubmitOutcome::Duplicate { turn_id } => {
            (StatusCode::ACCEPTED, Json(TurnCreated { turn_id })).into_response()
        }
        SubmitOutcome::Accepted { turn_id } => {
            let _ = append_and_project(
                &st.world,
                StreamEvent {
                    session_id,
                    seq: 0,
                    kind: EventKind::SessionBusy,
                    turn_id: Some(turn_id),
                    attempt: None,
                    payload: String::new(),
                },
            )
            .await;
            let tip = append_and_project(
                &st.world,
                StreamEvent {
                    session_id,
                    seq: 0,
                    kind: EventKind::TurnBegin,
                    turn_id: Some(turn_id),
                    attempt: None,
                    payload: body.text.clone(),
                },
            )
            .await;

            if let Ok(Some(mut snap)) = st.world.snapshot.get(session_id).await {
                snap.bubbles.push(Bubble {
                    role: "user".into(),
                    text: body.text,
                    turn_id: Some(turn_id),
                });
                snap.running = vec![turn_id];
                // Tip from append result — never read_from(1) (fails after hot trim).
                if let Ok(seq) = tip {
                    snap.snapshot_seq = seq;
                } else if let Some(seq) = st.world.stream.tip_seq(session_id) {
                    snap.snapshot_seq = seq;
                }
                let _ = st.world.snapshot.put(snap).await;
                project_snapshot_now(&st.world, session_id).await;
            }

            let resp = (StatusCode::ACCEPTED, Json(TurnCreated { turn_id })).into_response();
            if body.stream {
                // Convenience: clients that want same-connection SSE should use GET /stream;
                // we still return 202 JSON for simplicity in v1.
                return resp;
            }
            resp
        }
    }
}

async fn get_snapshot(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    let Ok(session_id) = Uuid::parse_str(&id).map(SessionId) else {
        return err(StatusCode::BAD_REQUEST, "invalid session_id".into());
    };
    if st.cfg.role == "edge" {
        if let Err(r) = ensure_mirrored(&st, session_id).await {
            return r;
        }
        return match st.world.mirror.get(session_id).await {
            Ok(Some(s)) => Json(s).into_response(),
            Ok(None) => err(StatusCode::NOT_FOUND, "no snapshot".into()),
            Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
    }
    match st.world.snapshot.get(session_id).await {
        Ok(Some(s)) => Json(s).into_response(),
        Ok(None) => err(StatusCode::NOT_FOUND, "no snapshot".into()),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[derive(Debug, Deserialize)]
struct StreamQuery {
    #[serde(default)]
    from_seq: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct EventsQuery {
    #[serde(default)]
    after_seq: Option<u64>,
    #[serde(default)]
    limit: Option<usize>,
}

/// Home (and edge-forward): batch events for mirror pull projection.
async fn list_events(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<EventsQuery>,
) -> Response {
    if st.cfg.role == "edge" {
        let after = q.after_seq.unwrap_or(0);
        let limit = q.limit.unwrap_or(1000);
        let path = format!("/v1/sessions/{id}/events?after_seq={after}&limit={limit}");
        return forward_raw(&st, "GET", &path).await;
    }
    let Ok(session_id) = Uuid::parse_str(&id).map(SessionId) else {
        return err(StatusCode::BAD_REQUEST, "invalid session_id".into());
    };
    let after = q.after_seq.unwrap_or(0);
    let limit = q.limit.unwrap_or(1000).min(10_000);
    let from = after.saturating_add(1);
    match st.world.stream.read_from(session_id, from, limit).await {
        Ok(events) => Json(serde_json::json!({ "events": events })).into_response(),
        Err(StreamError::Gap(g)) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "stream_gap",
                "recover_hint": g.hint,
                "requested_from": g.requested_from,
                "earliest_available": g.earliest_available,
            })),
        )
            .into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn stream_sse(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<StreamQuery>,
    headers: HeaderMap,
) -> Response {
    let Ok(session_id) = Uuid::parse_str(&id).map(SessionId) else {
        return err(StatusCode::BAD_REQUEST, "invalid session_id".into());
    };

    let mut from = q.from_seq.unwrap_or(1);
    if let Some(last) = headers.get("last-event-id").and_then(|v| v.to_str().ok()) {
        if let Ok(n) = last.parse::<u64>() {
            from = n + 1;
        }
    }

    if st.cfg.role == "edge" {
        if let Err(r) = ensure_mirrored(&st, session_id).await {
            return r;
        }
        return open_sse_from_mirror_edge(st, session_id, from).await;
    }

    open_sse_from_stream(st.world.stream.clone(), session_id, from).await
}

async fn open_sse_from_stream(
    stream: Arc<adapters_mem::MemStreamChannel>,
    session_id: SessionId,
    from: u64,
) -> Response {
    match stream.read_from(session_id, from, 1).await {
        Err(StreamError::Gap(g)) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "stream_gap",
                    "recover_hint": g.hint,
                    "requested_from": g.requested_from,
                    "earliest_available": g.earliest_available,
                })),
            )
                .into_response();
        }
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Ok(_) => {}
    }

    let start_after = from.saturating_sub(1);
    let s = stream::unfold(start_after, move |after| {
        let stream = stream.clone();
        async move {
            loop {
                match stream.read_after(session_id, after, 1).await {
                    Ok(batch) if !batch.is_empty() => {
                        let ev = batch.into_iter().next().unwrap();
                        let data = serde_json::to_string(&ev).unwrap_or_default();
                        let next_after = ev.seq;
                        return Some((
                            Ok::<_, Infallible>(Event::default().id(ev.seq.to_string()).data(data)),
                            next_after,
                        ));
                    }
                    Ok(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                    Err(StreamError::Gap(g)) => {
                        let data = serde_json::json!({
                            "error": "stream_gap",
                            "recover_hint": g.hint,
                        })
                        .to_string();
                        return Some((Ok(Event::default().event("error").data(data)), after));
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
        }
    });

    Sse::new(s)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

async fn open_sse_from_mirror_edge(
    st: AppState,
    session_id: SessionId,
    from: u64,
) -> Response {
    let mirror = st.world.mirror.clone();
    match mirror.read_from(session_id, from, 1).await {
        Err(StreamError::Gap(g)) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "stream_gap",
                    "recover_hint": g.hint,
                    "requested_from": g.requested_from,
                    "earliest_available": g.earliest_available,
                })),
            )
                .into_response();
        }
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        Ok(_) => {}
    }

    let start_after = from.saturating_sub(1);
    let s = stream::unfold(start_after, move |after| {
        let st = st.clone();
        let mirror = st.world.mirror.clone();
        async move {
            loop {
                match mirror.read_after(session_id, after, 1).await {
                    Ok(batch) if !batch.is_empty() => {
                        let ev = batch.into_iter().next().unwrap();
                        let data = serde_json::to_string(&ev).unwrap_or_default();
                        let next_after = ev.seq;
                        return Some((
                            Ok::<_, Infallible>(Event::default().id(ev.seq.to_string()).data(data)),
                            next_after,
                        ));
                    }
                    Ok(_) => {
                        let _ = ensure_mirrored(&st, session_id).await;
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err(StreamError::Gap(g)) => {
                        let data = serde_json::json!({
                            "error": "stream_gap",
                            "recover_hint": g.hint,
                        })
                        .to_string();
                        return Some((Ok(Event::default().event("error").data(data)), after));
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
        }
    });

    Sse::new(s)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

/// Pull home snapshot + events into local mirror (edge read path, V12).
async fn ensure_mirrored(st: &AppState, session_id: SessionId) -> Result<(), Response> {
    let Some(up) = &st.cfg.home_upstream else {
        return Err(err(StatusCode::BAD_GATEWAY, "no home_upstream".into()));
    };
    let sid = session_id.0;

    let snap_url = format!("http://{up}/v1/sessions/{sid}/snapshot");
    match st.http.get(&snap_url).send().await {
        Ok(resp) if resp.status().is_success() => {
            if let Ok(snap) = resp.json::<SessionSnapshot>().await {
                let _ = st.world.mirror.project_snapshot(snap);
            }
        }
        Ok(resp) if resp.status() == StatusCode::NOT_FOUND => {
            return Err(err(StatusCode::NOT_FOUND, "no snapshot".into()));
        }
        Ok(resp) => {
            return Err(err(
                StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
                format!("upstream snapshot {}", resp.status()),
            ));
        }
        Err(e) => return Err(err(StatusCode::BAD_GATEWAY, e.to_string())),
    }

    // Catch-up loop: if gap, jump to earliest_available.
    let mut after = st.world.mirror.tip_seq(session_id).unwrap_or(0);
    for _ in 0..8 {
        let ev_url = format!(
            "http://{up}/v1/sessions/{sid}/events?after_seq={after}&limit=10000"
        );
        match st.http.get(&ev_url).send().await {
            Ok(resp) if resp.status() == StatusCode::CONFLICT => {
                let v: serde_json::Value = resp.json().await.unwrap_or_default();
                if let Some(earliest) = v.get("earliest_available").and_then(|x| x.as_u64()) {
                    after = earliest.saturating_sub(1);
                    continue;
                }
                break;
            }
            Ok(resp) if resp.status().is_success() => {
                #[derive(Deserialize)]
                struct EventsResp {
                    events: Vec<StreamEvent>,
                }
                let body: EventsResp = match resp.json().await {
                    Ok(b) => b,
                    Err(_) => break,
                };
                if body.events.is_empty() {
                    break;
                }
                for e in body.events {
                    after = after.max(e.seq);
                    st.world.mirror.project_event(e);
                }
            }
            _ => break,
        }
    }
    Ok(())
}

// --- admin (home) ---

#[derive(Debug, Deserialize)]
struct ReadOnlyBody {
    enabled: bool,
}

async fn set_read_only(State(st): State<AppState>, Json(body): Json<ReadOnlyBody>) -> Response {
    if st.cfg.role != "home" {
        return err(StatusCode::FORBIDDEN, "read_only toggle only on home".into());
    }
    st.world.meta.set_read_only(body.enabled);
    Json(serde_json::json!({
        "ok": true,
        "read_only": st.world.meta.is_read_only(),
    }))
    .into_response()
}

#[derive(Debug, Deserialize)]
struct PendingLimitBody {
    pending_limit: usize,
}

async fn set_pending_limit(
    State(st): State<AppState>,
    Json(body): Json<PendingLimitBody>,
) -> Response {
    if st.cfg.role != "home" {
        return err(StatusCode::FORBIDDEN, "pending_limit only on home".into());
    }
    st.world.meta.set_pending_limit(body.pending_limit);
    Json(serde_json::json!({
        "ok": true,
        "pending_limit": st.world.meta.pending_limit(),
    }))
    .into_response()
}

#[derive(Debug, Deserialize)]
struct TrimHotBody {
    session_id: String,
    new_earliest: u64,
}

/// Fault / verification inject: unload hot below `new_earliest` (archives to mem cold).
async fn trim_hot(State(st): State<AppState>, Json(body): Json<TrimHotBody>) -> Response {
    if st.cfg.role != "home" {
        return err(StatusCode::FORBIDDEN, "trim_hot only on home".into());
    }
    let Ok(session_id) = Uuid::parse_str(&body.session_id).map(SessionId) else {
        return err(StatusCode::BAD_REQUEST, "invalid session_id".into());
    };
    st.world
        .stream
        .trim_earliest(session_id, body.new_earliest);
    st.world.mirror.project_trim(session_id, body.new_earliest);
    Json(serde_json::json!({
        "ok": true,
        "session_id": body.session_id,
        "new_earliest": body.new_earliest,
        "cold_len": st.world.stream.cold_len(session_id),
    }))
    .into_response()
}

// --- agent endpoints (home) ---

#[derive(Debug, Deserialize)]
struct ClaimBody {
    agent_id: Uuid,
}

#[derive(Serialize)]
struct ClaimResp {
    turn_id: TurnId,
    session_id: SessionId,
    attempt: Attempt,
    text: String,
    exec_deadline_ms: u64,
}

async fn agent_claim(State(st): State<AppState>, Json(body): Json<ClaimBody>) -> Response {
    if st.cfg.role != "home" {
        return err(StatusCode::FORBIDDEN, "claim only on home".into());
    }
    let agent = nova_sessions_core::AgentId(body.agent_id);
    match st.world.meta.claim_turn(agent, now_ms(), 3_600_000).await {
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Ok(Some(c)) => {
            let _ = append_and_project(
                &st.world,
                StreamEvent {
                    session_id: c.turn.session_id,
                    seq: 0,
                    kind: EventKind::AttemptStarted,
                    turn_id: Some(c.turn.turn_id),
                    attempt: Some(c.attempt),
                    payload: String::new(),
                },
            )
            .await;
            Json(ClaimResp {
                turn_id: c.turn.turn_id,
                session_id: c.turn.session_id,
                attempt: c.attempt,
                text: c.turn.text,
                exec_deadline_ms: c.exec_deadline_ms,
            })
            .into_response()
        }
        Err(nova_sessions_core::MetaError::ReadOnly) => {
            err(StatusCode::SERVICE_UNAVAILABLE, "read_only".into())
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[derive(Debug, Deserialize)]
struct HeartbeatBody {
    agent_id: Uuid,
}

async fn agent_heartbeat(State(st): State<AppState>, Json(body): Json<HeartbeatBody>) -> Response {
    let _ = st
        .world
        .meta
        .heartbeat(nova_sessions_core::AgentId(body.agent_id), now_ms())
        .await;
    StatusCode::NO_CONTENT.into_response()
}

#[derive(Debug, Deserialize)]
struct AppendBody {
    session_id: Uuid,
    turn_id: Uuid,
    attempt: u64,
    kind: String,
    payload: String,
}

async fn agent_append(State(st): State<AppState>, Json(body): Json<AppendBody>) -> Response {
    let kind = match body.kind.as_str() {
        "text_delta" => EventKind::TextDelta,
        "turn_done" => EventKind::TurnDone,
        "turn_failed" => EventKind::TurnFailed,
        other => return err(StatusCode::BAD_REQUEST, format!("unknown kind {other}")),
    };
    let session_id = SessionId(body.session_id);
    match append_and_project(
        &st.world,
        StreamEvent {
            session_id,
            seq: 0,
            kind,
            turn_id: Some(TurnId(body.turn_id)),
            attempt: Some(Attempt(body.attempt)),
            payload: body.payload,
        },
    )
    .await
    {
        Ok(seq) => {
            // Keep open-screen tip fresh mid-turn (FR-9): observers skip full hot replay.
            if let Ok(Some(mut snap)) = st.world.snapshot.get(session_id).await {
                if seq > snap.snapshot_seq {
                    snap.snapshot_seq = seq;
                    let _ = st.world.snapshot.put(snap).await;
                    project_snapshot_now(&st.world, session_id).await;
                }
            }
            Json(serde_json::json!({"seq": seq})).into_response()
        }
        Err(StreamError::StaleAttempt) => err(StatusCode::CONFLICT, "stale attempt".into()),
        Err(StreamError::ReadOnly) => err(StatusCode::SERVICE_UNAVAILABLE, "read_only".into()),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[derive(Debug, Deserialize)]
struct CompleteBody {
    session_id: Uuid,
    turn_id: Uuid,
    attempt: u64,
    #[serde(default = "default_ok")]
    ok: bool,
    #[serde(default)]
    assistant_text: String,
}

fn default_ok() -> bool {
    true
}

async fn agent_complete(State(st): State<AppState>, Json(body): Json<CompleteBody>) -> Response {
    let session_id = SessionId(body.session_id);
    let turn_id = TurnId(body.turn_id);
    let attempt = Attempt(body.attempt);
    let kind = if body.ok {
        EventKind::TurnDone
    } else {
        EventKind::TurnFailed
    };
    if let Err(e) = append_and_project(
        &st.world,
        StreamEvent {
            session_id,
            seq: 0,
            kind,
            turn_id: Some(turn_id),
            attempt: Some(attempt),
            payload: body.assistant_text.clone(),
        },
    )
    .await
    {
        return match e {
            StreamError::StaleAttempt => err(StatusCode::CONFLICT, "stale attempt".into()),
            StreamError::ReadOnly => err(StatusCode::SERVICE_UNAVAILABLE, "read_only".into()),
            e => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
    }
    let status = if body.ok {
        TurnStatus::Done
    } else {
        TurnStatus::Failed
    };
    if let Err(e) = st.world.meta.complete_turn(turn_id, attempt, status).await {
        return err(StatusCode::CONFLICT, e.to_string());
    }
    let tip = append_and_project(
        &st.world,
        StreamEvent {
            session_id,
            seq: 0,
            kind: EventKind::SessionIdle,
            turn_id: None,
            attempt: None,
            payload: String::new(),
        },
    )
    .await;

    if let Ok(Some(mut snap)) = st.world.snapshot.get(session_id).await {
        if body.ok && !body.assistant_text.is_empty() {
            snap.bubbles.push(Bubble {
                role: "assistant".into(),
                text: body.assistant_text,
                turn_id: Some(turn_id),
            });
        }
        snap.running.clear();
        if let Ok(seq) = tip {
            snap.snapshot_seq = seq;
        } else if let Some(seq) = st.world.stream.tip_seq(session_id) {
            snap.snapshot_seq = seq;
        }
        let _ = st.world.snapshot.put(snap).await;
        project_snapshot_now(&st.world, session_id).await;
    }
    StatusCode::NO_CONTENT.into_response()
}

// --- edge forward helpers ---

async fn forward_json<T: Serialize>(
    st: &AppState,
    method: &str,
    path: &str,
    body: Option<T>,
) -> Response {
    let Some(up) = &st.cfg.home_upstream else {
        return err(StatusCode::BAD_GATEWAY, "no home_upstream".into());
    };
    let url = format!("http://{up}{path}");
    let req = match method {
        "POST" => {
            let mut r = st.http.post(&url);
            if let Some(b) = body {
                r = r.json(&b);
            }
            r
        }
        _ => st.http.get(&url),
    };
    match req.send().await {
        Ok(resp) => {
            let status =
                StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let bytes = resp.bytes().await.unwrap_or_default();
            (status, bytes).into_response()
        }
        Err(e) => err(StatusCode::BAD_GATEWAY, e.to_string()),
    }
}

async fn forward_raw(st: &AppState, method: &str, path: &str) -> Response {
    forward_json(st, method, path, None::<()>).await
}

fn err(status: StatusCode, msg: String) -> Response {
    (status, Json(serde_json::json!({"error": msg}))).into_response()
}
