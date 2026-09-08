//! A real chat-completions scheduler over HTTP(S).

use async_trait::async_trait;
use futures::StreamExt;
use nova_agent_runtime::{AgentEventSink, SinkVerdict};
use nova_responses_core::Usage;

use crate::completions::{
    CompletionsOutcome, CompletionsRequest, FinishReason, ToolCall,
};
use crate::scheduler::{Scheduler, SchedulerError};

/// Calls `POST {base_url}/chat/completions` with the request's messages.
#[derive(Debug, Clone)]
pub struct HttpChatCompletionsScheduler {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    default_model: Option<String>,
}

impl HttpChatCompletionsScheduler {
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        default_model: Option<String>,
    ) -> Result<Self, String> {
        let base_url = base_url.into().trim().to_string();
        if base_url.is_empty() {
            return Err("base_url must not be empty".into());
        }
        let api_key = api_key.into().trim().to_string();
        if api_key.is_empty() {
            return Err("api_key must not be empty (check $NOVA_CHAT_API_KEY)".into());
        }
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key,
            default_model,
        })
    }

    fn model<'a>(&'a self, request: &'a CompletionsRequest) -> &'a str {
        self.default_model.as_deref().unwrap_or(&request.model)
    }

    fn body(&self, request: &CompletionsRequest) -> serde_json::Value {
        let mut body = serde_json::json!({
            "model": self.model(request),
            "messages": request.messages,
            "stream": true,
        });
        if let Some(t) = request.temperature {
            body["temperature"] = serde_json::json!(t);
        }
        if let Some(n) = request.max_completion_tokens {
            body["max_tokens"] = serde_json::json!(n);
        }
        if !request.tools.is_empty() {
            body["tools"] = serde_json::json!(request.tools);
        }
        if let Some(choice) = &request.tool_choice {
            body["tool_choice"] = serde_json::json!(choice);
        }
        body
    }

    fn estimated_usage(request: &CompletionsRequest, output: &str) -> Usage {
        Usage::new(
            (request.approx_input_chars() as u64 / 4).max(1),
            (output.len() as u64 / 4).max(1),
        )
    }
}

#[async_trait]
impl Scheduler for HttpChatCompletionsScheduler {
    fn name(&self) -> &str {
        "http"
    }

    async fn schedule(
        &self,
        request: &CompletionsRequest,
        sink: &mut dyn AgentEventSink,
    ) -> Result<CompletionsOutcome, SchedulerError> {
        let resp = self
            .client
            .post(format!("{}/chat/completions", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&self.body(request))
            .send()
            .await
            .map_err(|e| SchedulerError::Unavailable(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(match status {
                s if s.is_server_error() => SchedulerError::Unavailable(text),
                _ => SchedulerError::Rejected(text),
            });
        }

        let is_event_stream = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.contains("text/event-stream"))
            .unwrap_or(false);

        if is_event_stream {
            self.parse_stream(resp, request, sink).await
        } else {
            self.parse_json(resp, request).await
        }
    }
}

impl HttpChatCompletionsScheduler {
    async fn parse_json(
        &self,
        resp: reqwest::Response,
        request: &CompletionsRequest,
    ) -> Result<CompletionsOutcome, SchedulerError> {
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| SchedulerError::Unavailable(format!("decode json body: {e}")))?;

        let content = body["choices"][0]["message"]["content"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if content.is_empty() {
            return Err(SchedulerError::EmptyOutcome);
        }
        Ok(CompletionsOutcome::text(
            content.clone(),
            Self::estimated_usage(request, &content),
        ))
    }

    async fn parse_stream(
        &self,
        resp: reqwest::Response,
        request: &CompletionsRequest,
        sink: &mut dyn AgentEventSink,
    ) -> Result<CompletionsOutcome, SchedulerError> {
        let mut stream = resp.bytes_stream();
        let mut buffer = String::new();
        let mut text = String::new();
        let mut refusal: Option<String> = None;
        let mut finish = FinishReason::Stop;
        let mut tool_calls: Vec<ToolCall> = Vec::new();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| SchedulerError::Unavailable(e.to_string()))?;
            buffer.push_str(&String::from_utf8_lossy(&chunk));

            while let Some(nl) = buffer.find('\n') {
                let mut line: String = buffer.drain(..=nl).collect();
                while line.ends_with('\n') || line.ends_with('\r') {
                    line.pop();
                }
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let Some(data) = line.strip_prefix("data:") else {
                    continue;
                };
                let data = data.trim();
                if data == "[DONE]" {
                    break;
                }
                let Ok(event) = serde_json::from_str::<serde_json::Value>(data) else {
                    continue;
                };

                let choice = &event["choices"][0];
                if let Some(piece) = delta_text(choice) {
                    text.push_str(&piece);
                    if matches!(sink.text_delta(&piece).await?, SinkVerdict::Stop) {
                        return Err(SchedulerError::Superseded);
                    }
                }
                if let Some(piece) = reasoning_text(choice) {
                    if matches!(sink.reasoning_text_delta(&piece).await?, SinkVerdict::Stop) {
                        return Err(SchedulerError::Superseded);
                    }
                }
                if let Some(r) = refusal_text(choice) {
                    refusal.get_or_insert_with(String::new).push_str(&r);
                }
                accumulate_tool_calls(choice, &mut tool_calls);
                if let Some(reason) = choice["finish_reason"].as_str() {
                    finish = match reason {
                        "length" => FinishReason::Length,
                        "tool_calls" => FinishReason::ToolCalls,
                        _ => FinishReason::Stop,
                    };
                }
            }
        }

        if let Some(reason) = refusal {
            let usage = Self::estimated_usage(request, &reason);
            return Ok(CompletionsOutcome::refusal(reason, usage));
        }
        if !tool_calls.is_empty() {
            for call in &tool_calls {
                if matches!(
                    sink.tool_call(&call.id, &call.name, &call.arguments).await?,
                    SinkVerdict::Stop
                ) {
                    return Err(SchedulerError::Superseded);
                }
            }
            let args: String = tool_calls.iter().map(|c| c.arguments.as_str()).collect();
            let usage = Self::estimated_usage(request, &args);
            return Ok(CompletionsOutcome::tool_calls(tool_calls, usage));
        }
        if text.is_empty() {
            return Err(SchedulerError::EmptyOutcome);
        }
        let usage = Self::estimated_usage(request, &text);
        Ok(CompletionsOutcome {
            items: CompletionsOutcome::text(text.clone(), Usage::default()).items,
            usage,
            finish,
        })
    }
}

fn delta_text(choice: &serde_json::Value) -> Option<String> {
    let delta = choice.get("delta")?;
    if let Some(s) = delta.as_str() {
        return (!s.is_empty()).then(|| s.to_string());
    }
    if let Some(s) = delta["content"].as_str() {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    if let Some(s) = delta["content"]["text"].as_str() {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    if let Some(s) = delta["text"].as_str() {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    None
}

fn reasoning_text(choice: &serde_json::Value) -> Option<String> {
    let delta = choice.get("delta")?;
    for key in ["reasoning_content", "reasoning", "thinking"] {
        if let Some(s) = delta[key].as_str() {
            if !s.is_empty() {
                return Some(s.to_string());
            }
        }
    }
    None
}

fn refusal_text(choice: &serde_json::Value) -> Option<String> {
    let delta = choice.get("delta")?;
    if let Some(s) = delta["refusal"].as_str() {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    if let Some(s) = delta["refusal"]["text"].as_str() {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    None
}

fn accumulate_tool_calls(choice: &serde_json::Value, tool_calls: &mut Vec<ToolCall>) {
    let Some(delta) = choice.get("delta") else {
        return;
    };
    let Some(array) = delta["tool_calls"].as_array() else {
        return;
    };
    for tc in array {
        let index = tc["index"].as_u64().unwrap_or(0) as usize;
        while tool_calls.len() <= index {
            tool_calls.push(ToolCall {
                id: String::new(),
                name: String::new(),
                arguments: String::new(),
            });
        }
        let entry = &mut tool_calls[index];
        if let Some(id) = tc["id"].as_str() {
            if !id.is_empty() {
                entry.id = id.to_string();
            }
        }
        if let Some(name) = tc["function"]["name"].as_str() {
            if !name.is_empty() {
                entry.name = name.to_string();
            }
        }
        if let Some(args) = tc["function"]["arguments"].as_str() {
            entry.arguments.push_str(args);
        }
    }
}
