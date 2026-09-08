//! The orchestrator's event sink: appends agent events to the event log.

use std::sync::Arc;

use async_trait::async_trait;
use nova_responses_core::{
    AppendEvent, Attempt, EventBody, EventLogError, ResponseEventKind, ResponseEventLog,
    ResponseId, ResponseItem,
};

use crate::runner::{AgentEventSink, SinkError, SinkVerdict};

/// Streams agent events into this response's event log.
///
/// Owns the per-stream state (output index, current item id, content index,
/// accumulated reasoning) and the fencing `attempt`: an append refused as stale
/// flips `stopped`, and every later call returns [`SinkVerdict::Stop`].
pub struct EventSink {
    event_log: Arc<dyn ResponseEventLog>,
    response_id: ResponseId,
    attempt: Attempt,
    stopped: bool,
    /// Index of the output item currently being streamed, mirroring the
    /// OpenAI `response.output_item.*` events' `output_index`.
    output_index: u32,
    /// Id of the output item currently being streamed, carried on delta events
    /// as `item_id`.
    current_item_id: Option<String>,
    /// Index of the content part currently being streamed.
    current_content_index: Option<u32>,
    /// Reasoning / thinking text accumulated across the whole loop, persisted
    /// with the final output (render-only, never re-enters context).
    reasoning: String,
}

impl EventSink {
    pub fn new(
        event_log: Arc<dyn ResponseEventLog>,
        response_id: ResponseId,
        attempt: Attempt,
    ) -> Self {
        Self {
            event_log,
            response_id,
            attempt,
            stopped: false,
            output_index: 0,
            current_item_id: None,
            current_content_index: None,
            reasoning: String::new(),
        }
    }

    /// Whether the fence moved mid-generation (a later append was refused).
    pub fn stopped(&self) -> bool {
        self.stopped
    }

    /// Reasoning / thinking text accumulated across the whole loop.
    pub fn reasoning(&self) -> &str {
        &self.reasoning
    }

    async fn push(
        &mut self,
        kind: ResponseEventKind,
        body: EventBody,
    ) -> Result<SinkVerdict, SinkError> {
        if self.stopped {
            return Ok(SinkVerdict::Stop);
        }
        match self
            .event_log
            .append(AppendEvent {
                response_id: self.response_id.clone(),
                kind,
                attempt: Some(self.attempt),
                body,
            })
            .await
        {
            Ok(_) => Ok(SinkVerdict::Continue),
            Err(EventLogError::StaleAttempt) => {
                self.stopped = true;
                Ok(SinkVerdict::Stop)
            }
            Err(e) => Err(SinkError::Transport(e.to_string())),
        }
    }

    async fn push_item(
        &mut self,
        kind: ResponseEventKind,
        output_index: u32,
        item: &ResponseItem,
    ) -> Result<SinkVerdict, SinkError> {
        let item = serde_json::to_value(item).unwrap_or_default();
        self.push(kind, EventBody::Item { output_index, item })
            .await
    }
}

/// The stream identity of an output item: a tool `call_id` for tool items, the
/// message id otherwise.
fn item_id_of(item: &ResponseItem) -> String {
    match item {
        ResponseItem::FunctionCall { call_id, .. }
        | ResponseItem::FunctionCallOutput { call_id, .. } => call_id.clone(),
        ResponseItem::Message { id, .. } => id.clone().unwrap_or_default(),
    }
}

#[async_trait]
impl AgentEventSink for EventSink {
    async fn text_delta(&mut self, text: &str) -> Result<SinkVerdict, SinkError> {
        self.push(
            ResponseEventKind::OutputTextDelta,
            EventBody::Delta {
                item_id: self.current_item_id.clone().unwrap_or_default(),
                output_index: self.output_index.saturating_sub(1),
                content_index: self.current_content_index,
                delta: text.to_string(),
            },
        )
        .await
    }

    async fn reasoning_text_delta(&mut self, text: &str) -> Result<SinkVerdict, SinkError> {
        self.reasoning.push_str(text);
        self.push(
            ResponseEventKind::ReasoningTextDelta,
            EventBody::Delta {
                item_id: String::new(),
                output_index: 0,
                content_index: None,
                delta: text.to_string(),
            },
        )
        .await
    }

    async fn output_item_added(&mut self, item: &ResponseItem) -> Result<SinkVerdict, SinkError> {
        let index = self.output_index;
        self.output_index += 1;
        self.current_item_id = Some(item_id_of(item));
        self.push_item(ResponseEventKind::OutputItemAdded, index, item)
            .await
    }

    async fn output_item_done(&mut self, item: &ResponseItem) -> Result<SinkVerdict, SinkError> {
        let index = self.output_index.saturating_sub(1);
        self.push_item(ResponseEventKind::OutputItemDone, index, item)
            .await
    }

    async fn function_call_arguments_delta(
        &mut self,
        item_id: &str,
        delta: &str,
    ) -> Result<SinkVerdict, SinkError> {
        self.push(
            ResponseEventKind::FunctionCallArgumentsDelta,
            EventBody::Delta {
                item_id: item_id.to_string(),
                output_index: self.output_index.saturating_sub(1),
                content_index: None,
                delta: delta.to_string(),
            },
        )
        .await
    }

    async fn function_call_arguments_done(
        &mut self,
        item_id: &str,
        arguments: &str,
    ) -> Result<SinkVerdict, SinkError> {
        let index = self.output_index.saturating_sub(1);
        self.push(
            ResponseEventKind::FunctionCallArgumentsDone,
            EventBody::Arguments {
                output_index: index,
                item_id: item_id.to_string(),
                arguments: arguments.to_string(),
            },
        )
        .await
    }

    async fn content_part_added(
        &mut self,
        item_id: &str,
        content_index: u32,
    ) -> Result<SinkVerdict, SinkError> {
        self.current_content_index = Some(content_index);
        let part = serde_json::json!({ "type": "output_text", "text": "" });
        self.push(
            ResponseEventKind::ContentPartAdded,
            EventBody::Part {
                item_id: item_id.to_string(),
                output_index: self.output_index.saturating_sub(1),
                content_index,
                part,
            },
        )
        .await
    }

    async fn output_text_done(&mut self, text: &str) -> Result<SinkVerdict, SinkError> {
        self.push(
            ResponseEventKind::OutputTextDone,
            EventBody::Text {
                item_id: self.current_item_id.clone().unwrap_or_default(),
                output_index: self.output_index.saturating_sub(1),
                content_index: self.current_content_index.unwrap_or_default(),
                text: text.to_string(),
            },
        )
        .await
    }

    async fn content_part_done(
        &mut self,
        item_id: &str,
        content_index: u32,
        text: &str,
    ) -> Result<SinkVerdict, SinkError> {
        let part = serde_json::json!({ "type": "output_text", "text": text });
        self.push(
            ResponseEventKind::ContentPartDone,
            EventBody::Part {
                item_id: item_id.to_string(),
                output_index: self.output_index.saturating_sub(1),
                content_index,
                part,
            },
        )
        .await
    }
}
