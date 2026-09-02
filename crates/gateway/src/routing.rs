//! Two forwarding paths with fundamentally different natures (D21).
//!
//! | function | target | nature | direct connection |
//! |---|---|---|---|
//! | [`route_inflight`] | in-flight event buffer | **permanent architecture** | impossible — the state lives in one process's heap |
//! | [`route_content`] | context store | **temporary measure** | should be direct once storage is shared |
//!
//! Keeping them apart matters: describing both as "one directed hop" would let
//! chain affinity — a workaround for non-shared storage — calcify into a
//! permanent design element, and long conversations would then pin all their
//! traffic to a single node.
//!
//! Streaming requests are **proxied, never 307-redirected**: a redirect leaks
//! internal topology and the client may have no route to the host node behind a
//! load balancer.

use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use nova_responses_core::{NodeTag, ResponseId, TenantId};

use crate::state::AppState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// Handle here.
    Local,
    /// Proxy to this peer address.
    Peer(String),
    /// Tag is not in the registry. Callers must report "not found" and must
    /// **never** construct an address from the tag (SEC-5).
    UnknownNode,
}

/// Locate the node holding a response's in-flight event buffer.
///
/// This hop is permanent. The buffer is a `VecDeque` in a specific process's
/// heap; there is no shared endpoint that another node could connect to. That
/// is the deliberate cost of not putting 20k events/s through shared
/// middleware.
pub fn route_inflight(state: &AppState, id: &ResponseId) -> Route {
    resolve(state, id.node_tag())
}

/// Locate the node that can serve stored content.
///
/// Returns `Local` unconditionally when the store is shared, which is the whole
/// point of the `is_shared` capability flag: swapping in the SQL adapter retires
/// forwarding here without touching this code.
pub fn route_content(state: &AppState, id: &ResponseId) -> Route {
    if state.content_is_shared() {
        return Route::Local;
    }
    resolve(state, id.node_tag())
}

/// Chain affinity: send a chained create to the node owning the previous link,
/// so the whole walk stays local.
///
/// Only meaningful while content is not shared. Once it is, this **must** return
/// `Local`, otherwise a long conversation would nail every one of its turns to
/// one node and create a hotspot.
pub fn route_chain_affinity(state: &AppState, previous: &ResponseId) -> Route {
    if state.content_is_shared() {
        return Route::Local;
    }
    resolve(state, previous.node_tag())
}

fn resolve(state: &AppState, tag: &NodeTag) -> Route {
    if state.cfg.is_local(tag) {
        return Route::Local;
    }
    match state.cfg.peer_addr(tag) {
        Some(addr) => Route::Peer(addr.to_string()),
        None => Route::UnknownNode,
    }
}

/// Proxy a GET (including SSE) to a peer, preserving the query string.
pub async fn proxy_get(
    state: &AppState,
    peer: &str,
    path_and_query: &str,
    tenant: &TenantId,
    extra_headers: &HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    let url = format!("http://{peer}{path_and_query}");
    let mut req = state.http.get(&url);
    for (name, value) in state.keys.internal_headers(tenant) {
        req = req.header(name, value);
    }
    if let Some(last) = extra_headers.get("last-event-id") {
        req = req.header("last-event-id", last.clone());
    }
    let resp = req
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    relay(resp).await
}

/// Proxy a JSON body to a peer.
pub async fn proxy_post(
    state: &AppState,
    peer: &str,
    path_and_query: &str,
    tenant: &TenantId,
    body: &serde_json::Value,
) -> Result<Response, (StatusCode, String)> {
    let url = format!("http://{peer}{path_and_query}");
    let mut req = state.http.post(&url).json(body);
    for (name, value) in state.keys.internal_headers(tenant) {
        req = req.header(name, value);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    relay(resp).await
}

pub async fn proxy_delete(
    state: &AppState,
    peer: &str,
    path_and_query: &str,
    tenant: &TenantId,
) -> Result<Response, (StatusCode, String)> {
    let url = format!("http://{peer}{path_and_query}");
    let mut req = state.http.delete(&url);
    for (name, value) in state.keys.internal_headers(tenant) {
        req = req.header(name, value);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    relay(resp).await
}

/// Stream the peer's response straight through, preserving status,
/// content-type and — crucially for SSE — the incremental body.
async fn relay(resp: reqwest::Response) -> Result<Response, (StatusCode, String)> {
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_string();

    let body = axum::body::Body::from_stream(resp.bytes_stream());
    let mut out = Response::new(body);
    *out.status_mut() = status;
    out.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        content_type
            .parse()
            .unwrap_or_else(|_| axum::http::HeaderValue::from_static("application/json")),
    );
    Ok(out)
}
