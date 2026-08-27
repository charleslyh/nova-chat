//! Visual acceptance console for Session streaming API.

mod supervisor;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use clap::Parser;
use serde::Deserialize;
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;
use tracing::info;

use supervisor::{next_free_port_hint, parse_port, RegionRole, RegionSpec, SharedSim, SimSupervisor};

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:19090")]
    listen: String,
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
    SimSupervisor::ensure_bins()?;
    let sim: SharedSim = Arc::new(Mutex::new(SimSupervisor::new()?));

    let app = Router::new()
        .route("/", get(index))
        .route("/api/state", get(api_state))
        .route("/api/preset/two-region", post(api_preset))
        .route("/api/regions", post(api_add_region))
        .route("/api/regions/{id}", delete(api_remove_region))
        .route("/api/regions/{id}/start", post(api_start_region))
        .route("/api/regions/{id}/stop", post(api_stop_region))
        .route("/api/regions/{id}/agents", post(api_start_agent))
        .route("/api/agents/{id}", delete(api_stop_agent))
        .route("/api/sessions", post(api_create_session))
        .route("/api/sessions/{id}/turns", post(api_submit_turn))
        .route("/api/stop-all", post(api_stop_all))
        .layer(CorsLayer::permissive())
        .with_state(sim);

    let addr: SocketAddr = args.listen.parse()?;
    info!("Nova Sim console → http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn api_state(State(sim): State<SharedSim>) -> impl IntoResponse {
    let mut g = sim.lock().await;
    Json(g.snapshot().await)
}

async fn api_preset(State(sim): State<SharedSim>) -> Response {
    let mut g = sim.lock().await;
    match g.load_preset_two_region() {
        Ok(()) => {
            drop(g);
            let g = sim.lock().await;
            if let Err(e) = g.wait_healthy("home", Duration::from_secs(20)).await {
                return err(StatusCode::GATEWAY_TIMEOUT, e.to_string());
            }
            Json(serde_json::json!({"ok": true})).into_response()
        }
        Err(e) => err(StatusCode::BAD_REQUEST, e.to_string()),
    }
}

#[derive(Debug, Deserialize)]
struct AddRegionBody {
    id: String,
    role: String,
    #[serde(default)]
    listen: Option<String>,
    #[serde(default)]
    home_upstream: Option<String>,
}

async fn api_add_region(
    State(sim): State<SharedSim>,
    Json(body): Json<AddRegionBody>,
) -> Response {
    let mut g = sim.lock().await;
    let snap = g.snapshot().await;
    let used: Vec<u16> = snap
        .regions
        .iter()
        .filter_map(|r| parse_port(&r.listen))
        .collect();
    let port = next_free_port_hint(&used, 18080);
    let listen = body
        .listen
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| format!("127.0.0.1:{port}"));
    let role = match body.role.as_str() {
        "home" => RegionRole::Home,
        "edge" => RegionRole::Edge,
        _ => return err(StatusCode::BAD_REQUEST, "role must be home|edge".into()),
    };
    let mut home_upstream = body.home_upstream.filter(|s| !s.trim().is_empty());
    if matches!(role, RegionRole::Edge) && home_upstream.is_none() {
        home_upstream = snap
            .regions
            .iter()
            .find(|r| matches!(r.role, RegionRole::Home))
            .map(|r| r.listen.clone());
    }
    let spec = RegionSpec {
        id: body.id,
        role,
        listen,
        home_upstream,
    };
    match g.add_region(spec) {
        Ok(()) => Json(serde_json::json!({"ok": true})).into_response(),
        Err(e) => err(StatusCode::BAD_REQUEST, e.to_string()),
    }
}

async fn api_remove_region(State(sim): State<SharedSim>, Path(id): Path<String>) -> Response {
    let mut g = sim.lock().await;
    match g.remove_region(&id) {
        Ok(()) => Json(serde_json::json!({"ok": true})).into_response(),
        Err(e) => err(StatusCode::BAD_REQUEST, e.to_string()),
    }
}

async fn api_start_region(State(sim): State<SharedSim>, Path(id): Path<String>) -> Response {
    let mut g = sim.lock().await;
    match g.start_region(&id) {
        Ok(()) => {
            if let Err(e) = g.wait_healthy(&id, Duration::from_secs(15)).await {
                return err(StatusCode::GATEWAY_TIMEOUT, e.to_string());
            }
            Json(serde_json::json!({"ok": true})).into_response()
        }
        Err(e) => err(StatusCode::BAD_REQUEST, e.to_string()),
    }
}

async fn api_stop_region(State(sim): State<SharedSim>, Path(id): Path<String>) -> Response {
    let mut g = sim.lock().await;
    match g.stop_region(&id) {
        Ok(()) => Json(serde_json::json!({"ok": true})).into_response(),
        Err(e) => err(StatusCode::BAD_REQUEST, e.to_string()),
    }
}

#[derive(Debug, Deserialize)]
struct StartAgentBody {
    #[serde(default)]
    tokens: Option<usize>,
}

async fn api_start_agent(
    State(sim): State<SharedSim>,
    Path(id): Path<String>,
    Json(body): Json<StartAgentBody>,
) -> Response {
    let mut g = sim.lock().await;
    match g.start_agent(&id, body.tokens.unwrap_or(8)) {
        Ok(aid) => Json(serde_json::json!({"ok": true, "agent_id": aid})).into_response(),
        Err(e) => err(StatusCode::BAD_REQUEST, e.to_string()),
    }
}

async fn api_stop_agent(State(sim): State<SharedSim>, Path(id): Path<String>) -> Response {
    let mut g = sim.lock().await;
    match g.stop_agent(&id) {
        Ok(()) => Json(serde_json::json!({"ok": true})).into_response(),
        Err(e) => err(StatusCode::BAD_REQUEST, e.to_string()),
    }
}

#[derive(Debug, Deserialize)]
struct CreateSessionBody {
    #[serde(default)]
    region_id: Option<String>,
}

async fn api_create_session(
    State(sim): State<SharedSim>,
    Json(body): Json<CreateSessionBody>,
) -> Response {
    let mut g = sim.lock().await;
    match g.create_session(body.region_id.as_deref()).await {
        Ok(sid) => Json(serde_json::json!({"ok": true, "session_id": sid})).into_response(),
        Err(e) => err(StatusCode::BAD_REQUEST, e.to_string()),
    }
}

#[derive(Debug, Deserialize)]
struct TurnBody {
    text: String,
    #[serde(default)]
    region_id: Option<String>,
}

async fn api_submit_turn(
    State(sim): State<SharedSim>,
    Path(id): Path<String>,
    Json(body): Json<TurnBody>,
) -> Response {
    let mut g = sim.lock().await;
    match g
        .submit_turn(&id, &body.text, body.region_id.as_deref())
        .await
    {
        Ok(tid) => Json(serde_json::json!({"ok": true, "turn_id": tid})).into_response(),
        Err(e) => err(StatusCode::BAD_REQUEST, e.to_string()),
    }
}

async fn api_stop_all(State(sim): State<SharedSim>) -> Response {
    let mut g = sim.lock().await;
    g.stop_all();
    Json(serde_json::json!({"ok": true})).into_response()
}

fn err(status: StatusCode, msg: String) -> Response {
    (status, Json(serde_json::json!({"error": msg}))).into_response()
}
