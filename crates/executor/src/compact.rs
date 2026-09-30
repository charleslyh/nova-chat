//! Context compaction between a resolved response and its completion request.
//!
//! Conversation storage remains complete. This module keeps a bounded,
//! process-local compressed view, keyed by a previous response or conversation,
//! and applies the same four tiers as Nova's retired agentd implementation:
//! micro, snip, summary, then full.

use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use futures::StreamExt;
use nova_agent_runtime::AgentTask;
use nova_responses::{ConversationId, ResponseId, ResponseItem};

use crate::context::{append_items, items_to_messages, with_instructions};
use crate::wire::{
    ChatCompletion, ModelError, RequestMessage, ResponseChunk, ToolManifest, TransportOverrides,
};

const SUMMARY_BUDGET_TOKENS: usize = 12_000;
const AUTO_BUFFER_TOKENS: usize = 8_000;
const CACHE_CAPACITY: usize = 256;
const PLACEHOLDER: &str = "[old tool result cleared]";
const KEEP_RECENT_TOOL_RESULTS: usize = 2;
const SNIP_TRIM_COUNT: usize = 3;
const KEEP_RECENT_EXCHANGES: usize = 6;
const SUMMARY_PREFIX: &str = "[summary of earlier conversation]\n";
const FULL_PREFIX: &str = "[full conversation digest]\n";
const SUMMARIZER_PROMPT: &str = "You are a context compaction assistant. \
Summarize the following conversation between a user and an AI agent into a single \
concise paragraph in the same language as the conversation. Preserve concrete facts, \
decisions, file paths, identifiers, error messages, and unfinished tasks. \
Drop pleasantries and verbatim tool output. Output only the summary.";

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum CacheKey {
    Response(ResponseId),
    Conversation(ConversationId),
}

#[derive(Clone)]
struct CacheEntry {
    messages: Vec<RequestMessage>,
    /// Number of protocol items represented by `messages`. A later snapshot
    /// appends everything after this point, including the prior model output.
    covered_items: usize,
}

pub struct ContextCache {
    inner: Mutex<CacheInner>,
}

struct CacheInner {
    order: VecDeque<CacheKey>,
    entries: HashMap<CacheKey, Arc<CacheEntry>>,
}

impl ContextCache {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(CacheInner {
                order: VecDeque::new(),
                entries: HashMap::new(),
            }),
        }
    }

    fn get(&self, key: &CacheKey) -> Option<Arc<CacheEntry>> {
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let entry = Arc::clone(guard.entries.get(key)?);
        touch(&mut guard.order, key);
        Some(entry)
    }

    fn insert(&self, task: &AgentTask, entry: CacheEntry) {
        let mut keys = vec![CacheKey::Response(task.provenance.response_id.clone())];
        if let Some(id) = &task.conversation_id {
            keys.push(CacheKey::Conversation(id.clone()));
        }
        let entry = Arc::new(entry);
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        for key in keys {
            touch(&mut guard.order, &key);
            guard.entries.insert(key, Arc::clone(&entry));
        }
        while guard.order.len() > CACHE_CAPACITY {
            if let Some(stale) = guard.order.pop_front() {
                guard.entries.remove(&stale);
            }
        }
    }
}

impl Default for ContextCache {
    fn default() -> Self {
        Self::new()
    }
}

fn touch(order: &mut VecDeque<CacheKey>, key: &CacheKey) {
    if let Some(index) = order.iter().position(|candidate| candidate == key) {
        order.remove(index);
    }
    order.push_back(key.clone());
}

#[async_trait]
pub trait ConversationSummarizer: Send + Sync {
    async fn summarize(&self, messages: &[RequestMessage]) -> Result<String, ModelError>;
}

/// Uses the same provider, endpoint and per-turn transport overrides as the
/// completion that triggered compaction.
pub struct ModelSummarizer<'a> {
    provider: &'a dyn ChatCompletion,
    transport: &'a TransportOverrides,
}

impl<'a> ModelSummarizer<'a> {
    pub fn new(provider: &'a dyn ChatCompletion, transport: &'a TransportOverrides) -> Self {
        Self {
            provider,
            transport,
        }
    }
}

#[async_trait]
impl ConversationSummarizer for ModelSummarizer<'_> {
    async fn summarize(&self, messages: &[RequestMessage]) -> Result<String, ModelError> {
        if messages.is_empty() {
            return Ok(String::new());
        }
        let request = vec![
            RequestMessage::System {
                content: SUMMARIZER_PROMPT.to_string(),
            },
            RequestMessage::User {
                content: render_transcript(messages),
            },
        ];
        let mut stream = self
            .provider
            .completion(&request, &[], true, self.transport)
            .await?;
        let mut text = String::new();
        while let Some(chunk) = stream.next().await {
            match chunk? {
                ResponseChunk::TextBlock(block) => text.push_str(&block),
                ResponseChunk::Done { .. } => break,
                _ => {}
            }
        }
        Ok(text.trim().to_string())
    }
}

/// Build and, when configured, compact one model request.
pub async fn prepare_messages(
    cache: &ContextCache,
    task: &AgentTask,
    window_tokens: Option<usize>,
    tokenizer: Option<&str>,
    tools: &[ToolManifest],
    summarizer: &dyn ConversationSummarizer,
) -> Result<Vec<RequestMessage>, ModelError> {
    let mut conversation = cached_conversation(cache, task)
        .unwrap_or_else(|| full_conversation(&task.items))
        .or_else(|_| full_conversation(&task.items))?;

    if let Some(window) = window_tokens {
        let fixed = with_instructions(task.params.instructions.as_deref(), Vec::new());
        compact_in_place(
            &mut conversation,
            tools,
            &fixed,
            auto_threshold(window),
            estimator_for(tokenizer),
            summarizer,
        )
        .await?;
    }

    cache.insert(
        task,
        CacheEntry {
            messages: conversation.clone(),
            covered_items: task.items.len(),
        },
    );
    let messages = with_instructions(task.params.instructions.as_deref(), conversation);
    if messages.is_empty() {
        return Err(ModelError::new("response context contains no messages"));
    }
    Ok(messages)
}

fn cached_conversation(
    cache: &ContextCache,
    task: &AgentTask,
) -> Option<Result<Vec<RequestMessage>, ModelError>> {
    let key = task
        .previous_response_id
        .as_ref()
        .map(|id| CacheKey::Response(id.clone()))
        .or_else(|| {
            task.conversation_id
                .as_ref()
                .map(|id| CacheKey::Conversation(id.clone()))
        })?;
    let entry = cache.get(&key)?;
    if entry.covered_items > task.inherited_len || entry.covered_items > task.items.len() {
        return None;
    }
    let mut messages = entry.messages.clone();
    Some(append_items(&mut messages, &task.items[entry.covered_items..]).map(|()| messages))
}

fn full_conversation(items: &[ResponseItem]) -> Result<Vec<RequestMessage>, ModelError> {
    items_to_messages(items)
}

fn auto_threshold(window_tokens: usize) -> usize {
    window_tokens
        .saturating_sub(SUMMARY_BUDGET_TOKENS)
        .saturating_sub(AUTO_BUFFER_TOKENS)
}

#[derive(Clone, Debug, Default)]
struct TokenEstimate {
    total: usize,
    message_tokens: Vec<usize>,
}

type TokenEstimator =
    dyn Fn(&[RequestMessage], &[ToolManifest], &[RequestMessage]) -> TokenEstimate + Send + Sync;

struct TokenLedger {
    estimator: Arc<TokenEstimator>,
    tools: Vec<ToolManifest>,
    fixed: Vec<RequestMessage>,
    current: usize,
    message_tokens: Vec<usize>,
}

impl TokenLedger {
    fn new(
        estimator: Arc<TokenEstimator>,
        tools: &[ToolManifest],
        fixed: &[RequestMessage],
    ) -> Self {
        Self {
            estimator,
            tools: tools.to_vec(),
            fixed: fixed.to_vec(),
            current: 0,
            message_tokens: Vec::new(),
        }
    }

    fn sync(&mut self, messages: &[RequestMessage]) {
        let estimate = (self.estimator)(messages, &self.tools, &self.fixed);
        self.current = estimate.total;
        self.message_tokens = estimate.message_tokens;
    }

    fn clear_index(&mut self, index: usize) -> usize {
        let Some(tokens) = self.message_tokens.get_mut(index) else {
            return 0;
        };
        let freed = *tokens;
        *tokens = 0;
        self.current = self.current.saturating_sub(freed);
        freed
    }

    fn replace_range(&mut self, range: Range<usize>, replacement_count: usize) -> usize {
        if range.start > range.end || range.end > self.message_tokens.len() {
            return 0;
        }
        let freed = self.message_tokens[range.clone()].iter().sum();
        self.message_tokens
            .splice(range, std::iter::repeat_n(0, replacement_count));
        self.current = self.current.saturating_sub(freed);
        freed
    }
}

fn estimator_for(tokenizer: Option<&str>) -> Arc<TokenEstimator> {
    match tokenizer {
        Some("hy3") => Arc::new(Hy3TokenEstimator::estimate),
        _ => Arc::new(ConservativeTokenEstimator::estimate),
    }
}

struct ConservativeTokenEstimator;

impl ConservativeTokenEstimator {
    fn count_text(text: &str) -> usize {
        // One Unicode scalar per token deliberately overestimates typical
        // Latin text and does not undercount CJK as chars/4 would.
        text.chars().count()
    }

    fn estimate(
        messages: &[RequestMessage],
        tools: &[ToolManifest],
        fixed: &[RequestMessage],
    ) -> TokenEstimate {
        estimate_with(Self::count_text, messages, tools, fixed)
    }
}

struct Hy3TokenEstimator;

impl Hy3TokenEstimator {
    fn tokenizer() -> &'static tokenizers::Tokenizer {
        static TOKENIZER: OnceLock<tokenizers::Tokenizer> = OnceLock::new();
        TOKENIZER.get_or_init(|| {
            tokenizers::Tokenizer::from_bytes(include_bytes!("../assets/hy3/tokenizer.json"))
                .expect("Hy3 tokenizer.json is not a Hugging Face tokenizer")
        })
    }

    fn count_text(text: &str) -> usize {
        if text.is_empty() {
            return 0;
        }
        Self::tokenizer()
            .encode(text, false)
            .map(|encoding| encoding.len())
            .unwrap_or_else(|_| ConservativeTokenEstimator::count_text(text))
    }

    fn estimate(
        messages: &[RequestMessage],
        tools: &[ToolManifest],
        fixed: &[RequestMessage],
    ) -> TokenEstimate {
        estimate_with(Self::count_text, messages, tools, fixed)
    }
}

fn estimate_with(
    count: fn(&str) -> usize,
    messages: &[RequestMessage],
    tools: &[ToolManifest],
    fixed: &[RequestMessage],
) -> TokenEstimate {
    let per_message: Vec<usize> = messages
        .iter()
        .map(|message| message_tokens(message, count))
        .collect();
    let fixed_tokens: usize = fixed
        .iter()
        .map(|message| message_tokens(message, count))
        .sum();
    let tool_tokens: usize = tools
        .iter()
        .map(|tool| {
            count(&tool.name) + count(&tool.description) + count(&tool.parameters.to_string())
        })
        .sum();
    TokenEstimate {
        total: per_message.iter().sum::<usize>() + fixed_tokens + tool_tokens,
        message_tokens: per_message,
    }
}

fn message_tokens(message: &RequestMessage, count: fn(&str) -> usize) -> usize {
    match message {
        RequestMessage::System { content }
        | RequestMessage::User { content }
        | RequestMessage::Tool { content, .. } => count(content),
        RequestMessage::Assistant {
            content,
            tool_calls,
        } => {
            let mut tokens = count(content);
            if let Some(calls) = tool_calls {
                for call in calls {
                    tokens += count(&call.name) + count(&call.arguments.to_string());
                }
            }
            tokens
        }
    }
}

async fn compact_in_place(
    messages: &mut Vec<RequestMessage>,
    tools: &[ToolManifest],
    fixed: &[RequestMessage],
    threshold: usize,
    estimator: Arc<TokenEstimator>,
    summarizer: &dyn ConversationSummarizer,
) -> Result<(), ModelError> {
    let mut ledger = TokenLedger::new(estimator, tools, fixed);
    ledger.sync(messages);

    if micro(messages, &mut ledger, threshold) {
        ledger.sync(messages);
    }
    if snip(messages, &mut ledger, threshold) {
        ledger.sync(messages);
    }
    if summary(messages, &mut ledger, threshold, summarizer).await? {
        ledger.sync(messages);
    }
    if full(messages, &mut ledger, threshold, summarizer).await? {
        ledger.sync(messages);
    }
    Ok(())
}

fn micro(messages: &mut [RequestMessage], ledger: &mut TokenLedger, threshold: usize) -> bool {
    if ledger.current < threshold {
        return false;
    }
    let mut tool_seen = 0usize;
    let mut mutated = false;
    for (index, message) in messages.iter_mut().enumerate().rev() {
        if let RequestMessage::Tool { content, .. } = message {
            tool_seen += 1;
            if tool_seen > KEEP_RECENT_TOOL_RESULTS && content != PLACEHOLDER {
                *content = PLACEHOLDER.to_string();
                ledger.clear_index(index);
                mutated = true;
            }
        }
    }
    mutated
}

fn snip(messages: &mut Vec<RequestMessage>, ledger: &mut TokenLedger, threshold: usize) -> bool {
    if ledger.current < threshold || exchange_count(messages) <= SNIP_TRIM_COUNT {
        return false;
    }
    let mut trimmed = 0usize;
    for _ in 0..SNIP_TRIM_COUNT {
        if ledger.current < threshold {
            break;
        }
        let Some(range) = oldest_exchange_range(messages) else {
            break;
        };
        messages.drain(range);
        ledger.sync(messages);
        trimmed += 1;
    }
    trimmed > 0
}

async fn summary(
    messages: &mut Vec<RequestMessage>,
    ledger: &mut TokenLedger,
    threshold: usize,
    summarizer: &dyn ConversationSummarizer,
) -> Result<bool, ModelError> {
    if ledger.current < threshold {
        return Ok(false);
    }
    let system_offset = leading_system_len(messages);
    let Some(current) = last_exchange_start(messages) else {
        return Ok(false);
    };
    if current <= system_offset {
        return Ok(false);
    }
    let split = recent_exchange_start(messages, KEEP_RECENT_EXCHANGES).unwrap_or(current);
    if split <= system_offset {
        return Ok(false);
    }
    let text = summarizer
        .summarize(&messages[system_offset..split])
        .await?;
    if text.trim().is_empty() {
        return Ok(false);
    }
    let recent = messages.split_off(split);
    messages.truncate(system_offset);
    messages.push(RequestMessage::User {
        content: format!("{SUMMARY_PREFIX}{text}"),
    });
    messages.extend(recent);
    ledger.replace_range(system_offset..split, 1);
    Ok(true)
}

async fn full(
    messages: &mut Vec<RequestMessage>,
    ledger: &mut TokenLedger,
    threshold: usize,
    summarizer: &dyn ConversationSummarizer,
) -> Result<bool, ModelError> {
    if ledger.current < threshold {
        return Ok(false);
    }
    let system_offset = leading_system_len(messages);
    let Some(current) = last_exchange_start(messages) else {
        return Ok(false);
    };
    if current <= system_offset {
        return Ok(false);
    }
    let text = summarizer
        .summarize(&messages[system_offset..current])
        .await?;
    if text.trim().is_empty() {
        return Ok(false);
    }
    let current_exchange = messages.split_off(current);
    messages.truncate(system_offset);
    messages.push(RequestMessage::User {
        content: format!("{FULL_PREFIX}{text}"),
    });
    messages.extend(current_exchange);
    ledger.replace_range(system_offset..current, 1);
    Ok(true)
}

fn leading_system_len(messages: &[RequestMessage]) -> usize {
    messages
        .iter()
        .take_while(|message| matches!(message, RequestMessage::System { .. }))
        .count()
}

fn last_exchange_start(messages: &[RequestMessage]) -> Option<usize> {
    messages
        .iter()
        .rposition(|message| matches!(message, RequestMessage::User { .. }))
}

fn recent_exchange_start(messages: &[RequestMessage], keep: usize) -> Option<usize> {
    let indexes: Vec<usize> = messages
        .iter()
        .enumerate()
        .filter_map(|(index, message)| {
            matches!(message, RequestMessage::User { .. }).then_some(index)
        })
        .collect();
    (indexes.len() > keep).then(|| indexes[indexes.len() - keep])
}

fn exchange_count(messages: &[RequestMessage]) -> usize {
    messages
        .iter()
        .filter(|message| matches!(message, RequestMessage::User { .. }))
        .count()
}

fn oldest_exchange_range(messages: &[RequestMessage]) -> Option<Range<usize>> {
    let start = messages
        .iter()
        .position(|message| matches!(message, RequestMessage::User { .. }))?;
    let end = messages
        .iter()
        .enumerate()
        .skip(start + 1)
        .find_map(|(index, message)| {
            matches!(message, RequestMessage::User { .. }).then_some(index)
        })?;
    Some(start..end)
}

fn render_transcript(messages: &[RequestMessage]) -> String {
    let mut out = String::new();
    for message in messages {
        match message {
            RequestMessage::System { content } => {
                out.push_str("[system]\n");
                out.push_str(content);
            }
            RequestMessage::User { content } => {
                out.push_str("[user]\n");
                out.push_str(content);
            }
            RequestMessage::Assistant {
                content,
                tool_calls,
            } => {
                out.push_str("[assistant]\n");
                out.push_str(content);
                if let Some(calls) = tool_calls {
                    for call in calls {
                        out.push_str("\n<tool_call name=");
                        out.push_str(&call.name);
                        out.push_str(" args=");
                        out.push_str(&call.arguments.to_string());
                        out.push('>');
                    }
                }
            }
            RequestMessage::Tool { content, call_id } => {
                out.push_str("[tool ");
                out.push_str(call_id);
                out.push_str("]\n");
                out.push_str(content);
            }
        }
        out.push_str("\n\n");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use nova_responses::{Attempt, NodeTag};

    struct FixedSummary(&'static str);

    #[async_trait]
    impl ConversationSummarizer for FixedSummary {
        async fn summarize(&self, _messages: &[RequestMessage]) -> Result<String, ModelError> {
            Ok(self.0.to_string())
        }
    }

    fn response_id() -> ResponseId {
        ResponseId::new(NodeTag::parse("test").expect("tag"))
    }

    fn task(
        items: Vec<ResponseItem>,
        inherited_len: usize,
        previous_response_id: Option<ResponseId>,
        conversation_id: Option<ConversationId>,
    ) -> AgentTask {
        let mut params = nova_responses::ModelParams::new("m");
        params.instructions = Some("stay brief".into());
        AgentTask {
            params,
            exec_context: None,
            items,
            inherited_len,
            previous_response_id,
            conversation_id,
            provenance: nova_responses::RequestProvenance {
                response_id: response_id(),
                attempt: Attempt::UNCLAIMED.next(),
                exec_deadline_ms: u64::MAX,
            },
        }
    }

    #[test]
    fn hy3_tokenizer_counts_chinese_and_tool_schema() {
        assert_eq!(Hy3TokenEstimator::count_text("北京今天晴"), 3);
        let messages = vec![RequestMessage::User {
            content: "北京今天晴".into(),
        }];
        let tools = vec![ToolManifest {
            name: "get_weather".into(),
            description: "查询天气".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {"city": {"type": "string"}}
            }),
        }];
        let estimate = Hy3TokenEstimator::estimate(&messages, &tools, &[]);
        assert!(estimate.total > 3);
    }

    #[test]
    fn fallback_is_conservative_for_cjk() {
        let text = "北京今天晴";
        assert_eq!(
            ConservativeTokenEstimator::count_text(text),
            text.chars().count()
        );
        assert!(
            ConservativeTokenEstimator::count_text(text) >= Hy3TokenEstimator::count_text(text)
        );
    }

    #[test]
    fn automatic_threshold_reserves_summary_and_buffer() {
        assert_eq!(auto_threshold(262_144), 242_144);
    }

    #[tokio::test]
    async fn four_tiers_keep_the_current_exchange() {
        let mut messages = vec![RequestMessage::System {
            content: "preamble".into(),
        }];
        messages.extend((0..10).map(|index| RequestMessage::User {
            content: format!("turn-{index}{}", "x".repeat(40)),
        }));
        compact_in_place(
            &mut messages,
            &[],
            &[],
            1,
            Arc::new(ConservativeTokenEstimator::estimate),
            &FixedSummary("digest"),
        )
        .await
        .expect("compact");
        assert!(matches!(
            messages.last(),
            Some(RequestMessage::User { content }) if content.starts_with("turn-9")
        ));
        assert!(messages.iter().any(|message| matches!(
            message,
            RequestMessage::User { content }
                if content.starts_with(FULL_PREFIX) || content.starts_with(SUMMARY_PREFIX)
        )));
    }

    #[tokio::test]
    async fn blank_summary_leaves_history_for_the_next_tier() {
        let mut messages = vec![
            RequestMessage::User {
                content: "old".into(),
            },
            RequestMessage::Assistant {
                content: "answer".into(),
                tool_calls: None,
            },
            RequestMessage::User {
                content: "current".into(),
            },
        ];
        let before = messages.clone();
        let estimator = Arc::new(ConservativeTokenEstimator::estimate);
        let mut ledger = TokenLedger::new(estimator, &[], &[]);
        ledger.sync(&messages);
        assert!(!summary(&mut messages, &mut ledger, 1, &FixedSummary("  "))
            .await
            .expect("summary"));
        assert_eq!(messages, before);
    }

    #[tokio::test]
    async fn previous_response_cache_appends_prior_output_and_new_input() {
        let cache = ContextCache::new();
        let summary = FixedSummary("");
        let mut first = task(vec![ResponseItem::user_text("one")], 0, None, None);
        let first_id = response_id();
        first.provenance.response_id = first_id.clone();
        prepare_messages(&cache, &first, None, None, &[], &summary)
            .await
            .expect("first");

        let second = task(
            vec![
                ResponseItem::user_text("one"),
                ResponseItem::assistant_text("answer"),
                ResponseItem::user_text("two"),
            ],
            2,
            Some(first_id),
            None,
        );
        let prepared = prepare_messages(&cache, &second, None, None, &[], &summary)
            .await
            .expect("second");
        assert!(prepared.iter().any(|message| matches!(
            message,
            RequestMessage::Assistant { content, .. } if content == "answer"
        )));
        assert!(prepared.iter().any(|message| matches!(
            message,
            RequestMessage::User { content } if content == "two"
        )));
    }

    #[tokio::test]
    async fn conversation_anchor_reuses_the_cached_prefix() {
        let cache = ContextCache::new();
        let summary = FixedSummary("");
        let conversation = ConversationId::new();
        let first = task(
            vec![ResponseItem::user_text("one")],
            0,
            None,
            Some(conversation.clone()),
        );
        prepare_messages(&cache, &first, None, None, &[], &summary)
            .await
            .expect("first");
        let second = task(
            vec![
                ResponseItem::user_text("one"),
                ResponseItem::assistant_text("answer"),
                ResponseItem::user_text("two"),
            ],
            2,
            None,
            Some(conversation),
        );
        let prepared = prepare_messages(&cache, &second, None, None, &[], &summary)
            .await
            .expect("second");
        assert_eq!(
            prepared
                .iter()
                .filter(|message| matches!(message, RequestMessage::User { content } if content == "one"))
                .count(),
            1
        );
    }

    #[test]
    fn lru_evicts_the_oldest_key() {
        let cache = ContextCache::new();
        let first_id = response_id();
        for index in 0..=CACHE_CAPACITY {
            let mut value = task(vec![], 0, None, None);
            if index == 0 {
                value.provenance.response_id = first_id.clone();
            }
            cache.insert(
                &value,
                CacheEntry {
                    messages: Vec::new(),
                    covered_items: 0,
                },
            );
        }
        assert!(cache.get(&CacheKey::Response(first_id)).is_none());
    }
}
