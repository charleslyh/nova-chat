//! Minimal JSON-over-HTTP transport for the data plane.

use mock_server::proto::{Request, Response};

/// A transport-level failure (connect, timeout, bad status, decode). The
/// adapters map this onto their own error type.
pub struct TransportError(pub String);

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

pub struct Rpc {
    client: reqwest::Client,
    url: String,
}

impl Rpc {
    pub fn new(base: &str) -> Self {
        let url = format!("{}/rpc", base.trim_end_matches('/'));
        let client = reqwest::Client::builder()
            // Generous ceiling: `read_after` long-polls block up to its `wait_ms`,
            // which must stay well below this.
            .timeout(std::time::Duration::from_secs(120))
            .build()
            .expect("build reqwest client");
        Self { client, url }
    }

    pub async fn call(&self, req: Request) -> Result<Response, TransportError> {
        let resp = self
            .client
            .post(&self.url)
            .json(&req)
            .send()
            .await
            .map_err(|e| TransportError(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(TransportError(format!("rpc {status}: {body}")));
        }
        resp.json::<Response>()
            .await
            .map_err(|e| TransportError(e.to_string()))
    }
}
