//! The orchestrator's event sink: appends agent events to the event log.

use std::sync::Arc;

use async_trait::async_trait;
use nova_responses::ports::{EventLogError, ResponseEventLog};
use nova_responses::{AppendEvent, Attempt, ContentPart, ResponseId, ResponseItem};

use crate::runner::{AgentEventSink, SinkError, SinkVerdict};

/// Streams agent events into this response's event log.
///
/// Owns the per-stream state (output index, current item id, content index, accumulated
/// reasoning) and the fencing `attempt`: an append refused as stale flips `stopped`, and
/// every later call returns [`SinkVerdict::Stop`].
pub struct EventSink {
    event_log: Arc<dyn ResponseEventLog>,
    response_id: ResponseId,
    attempt: Attempt,
    stopped: bool,
    /// Index of the output item currently being streamed, mirroring the OpenAI
    /// `response.output_item.*` events' `output_index`.
    output_index: u32,
    /// Id of the output item currently being streamed, carried on delta events as
    /// `item_id`.
    current_item_id: Option<String>,
    /// Index of the content part currently being streamed.
    current_content_index: Option<u32>,
    /// Reasoning / thinking text accumulated across the whole loop, persisted with the
    /// final output (render-only, never re-enters context).
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

    fn id(&self) -> ResponseId {
        self.response_id.clone()
    }

    /// The index of the item currently open. `output_index` counts items *added*, so
    /// the open one is the previous number.
    fn current_output_index(&self) -> u32 {
        self.output_index.saturating_sub(1)
    }

    fn current_item(&self) -> String {
        self.current_item_id.clone().unwrap_or_default()
    }

    /// Append one already-well-formed event.
    ///
    /// Every caller below builds its event through an [`AppendEvent`] constructor, so
    /// there is no path here that can pair an event name with a body that does not
    /// belong to it.
    async fn push(&mut self, event: AppendEvent) -> Result<SinkVerdict, SinkError> {
        if self.stopped {
            return Ok(SinkVerdict::Stop);
        }
        match self.event_log.append(event).await {
            Ok(_) => Ok(SinkVerdict::Continue),
            Err(EventLogError::StaleAttempt) => {
                self.stopped = true;
                Ok(SinkVerdict::Stop)
            }
            Err(e) => Err(SinkError::Transport(e.to_string())),
        }
    }
}

#[async_trait]
impl AgentEventSink for EventSink {
    async fn text_delta(&mut self, text: &str) -> Result<SinkVerdict, SinkError> {
        let event = AppendEvent::text_delta(
            self.id(),
            self.attempt,
            self.current_item(),
            self.current_output_index(),
            self.current_content_index.unwrap_or_default(),
            text,
        );
        self.push(event).await
    }

    async fn reasoning_text_delta(&mut self, text: &str) -> Result<SinkVerdict, SinkError> {
        self.reasoning.push_str(text);
        let event = AppendEvent::reasoning_text_delta(self.id(), self.attempt, text);
        self.push(event).await
    }

    async fn output_item_added(&mut self, item: &ResponseItem) -> Result<SinkVerdict, SinkError> {
        let index = self.output_index;
        self.output_index += 1;
        self.current_item_id = Some(item.stream_item_id().to_string());
        let event = AppendEvent::item(self.id(), self.attempt, false, index, item.clone());
        self.push(event).await
    }

    async fn output_item_done(&mut self, item: &ResponseItem) -> Result<SinkVerdict, SinkError> {
        let event = AppendEvent::item(
            self.id(),
            self.attempt,
            true,
            self.current_output_index(),
            item.clone(),
        );
        self.push(event).await
    }

    async fn function_call_arguments_delta(
        &mut self,
        item_id: &str,
        delta: &str,
    ) -> Result<SinkVerdict, SinkError> {
        let event = AppendEvent::arguments_delta(
            self.id(),
            self.attempt,
            item_id,
            self.current_output_index(),
            delta,
        );
        self.push(event).await
    }

    async fn function_call_arguments_done(
        &mut self,
        item_id: &str,
        arguments: &str,
    ) -> Result<SinkVerdict, SinkError> {
        let event = AppendEvent::arguments(
            self.id(),
            self.attempt,
            self.current_output_index(),
            item_id,
            arguments,
        );
        self.push(event).await
    }

    async fn content_part_added(
        &mut self,
        item_id: &str,
        content_index: u32,
    ) -> Result<SinkVerdict, SinkError> {
        self.current_content_index = Some(content_index);
        let event = AppendEvent::content_part(
            self.id(),
            self.attempt,
            false,
            item_id,
            self.current_output_index(),
            content_index,
            ContentPart::OutputText {
                text: String::new(),
            },
        );
        self.push(event).await
    }

    async fn output_text_done(&mut self, text: &str) -> Result<SinkVerdict, SinkError> {
        let event = AppendEvent::output_text_done(
            self.id(),
            self.attempt,
            self.current_item(),
            self.current_output_index(),
            self.current_content_index.unwrap_or_default(),
            text,
        );
        self.push(event).await
    }

    async fn content_part_done(
        &mut self,
        item_id: &str,
        content_index: u32,
        text: &str,
    ) -> Result<SinkVerdict, SinkError> {
        let event = AppendEvent::content_part(
            self.id(),
            self.attempt,
            true,
            item_id,
            self.current_output_index(),
            content_index,
            ContentPart::OutputText {
                text: text.to_string(),
            },
        );
        self.push(event).await
    }
}
