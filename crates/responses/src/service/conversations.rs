//! Conversations 能力层：用例编排，无 axum 依赖。
//!
//! 会话是对话内容的**长期权威来源**（D30）：它存链尾指针、轮次锁、事件流，以及
//! 物化的主快照（每轮 delta 追加）。`transcript` 与生成入口的上下文装配都从这里读。
//! 本层负责 CRUD 编排，以及把会话标识解析为链尾 `ResponseId`——这是 `/v1/responses`
//! 携带 `conversation` 时唯一需要的解析步骤，之后上下文装配走 `read_snapshot`。

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{
    Conversation, ConversationError, ConversationEvent, ConversationEventKind, ConversationId,
    ConversationStore, MetricsSink, ResolvedContext, ResponseId, ResponseItem, ResponseStatus,
    TenantId, Usage,
};

/// 会话容器的链尾解析结果。
///
/// 区分「容器不存在」与「容器存在但还没有任何轮次」很重要：前者是调用方错误
/// （404 / 400），后者是首轮的正常起点（空上下文）。合并成 `Option<ResponseId>`
/// 会把两者混为一谈，让首轮和打错的 id 表现相同。
#[derive(Debug, Clone, PartialEq)]
pub enum ConversationTail {
    /// 尚无轮次完成，本次是首轮，上下文为空。
    Empty,
    /// 链尾响应，作为本次生成的上下文锚点。
    At(ResponseId),
}

/// 读取对话历史的失败原因。
#[derive(Debug, thiserror::Error)]
pub enum TranscriptError {
    #[error(transparent)]
    Conversation(#[from] ConversationError),
}

/// Conversations 用例编排。
pub struct ConversationsService {
    conversations: Arc<dyn ConversationStore>,
    now: Arc<dyn Fn() -> u64 + Send + Sync>,
    metrics: Arc<dyn MetricsSink>,
}

impl ConversationsService {
    pub fn new(
        conversations: Arc<dyn ConversationStore>,
        now: Arc<dyn Fn() -> u64 + Send + Sync>,
        metrics: Arc<dyn MetricsSink>,
    ) -> Self {
        Self {
            conversations,
            now,
            metrics,
        }
    }

    pub async fn create(
        &self,
        tenant: &TenantId,
        metadata: BTreeMap<String, String>,
    ) -> Result<Conversation, ConversationError> {
        // 不写前探活（D28）：库不可用由失败返回错误直接暴露，低概率失败用「治疗」
        // 而非「预防」。启动探活（fail-fast）仍在 gateway 装配处。
        let now_ms = (self.now)();
        let conversation = Conversation::new(
            ConversationId::new(),
            tenant.clone(),
            metadata,
            now_ms,
        );
        let created = self.conversations.create(conversation).await?;
        self.metrics.incr("conversations_created", 1).await;
        Ok(created)
    }

    pub async fn retrieve(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<Option<Conversation>, ConversationError> {
        self.conversations.get(tenant, id).await
    }

    pub async fn update_metadata(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        metadata: BTreeMap<String, String>,
    ) -> Result<Conversation, ConversationError> {
        self.conversations
            .update_metadata(tenant, id, metadata)
            .await
    }

    /// 删除容器。**不级联删除会话快照里的内容？**——D30 下快照就是会话自己的内容，
    /// 删会话即删快照；但响应记录不级联（与 D24 的记录级删除、官方「Items in the
    /// conversation will not be deleted」的措辞需按 D30 语义重估，见 D30 正文）。
    pub async fn delete(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<bool, ConversationError> {
        let deleted = self.conversations.delete(tenant, id).await?;
        if deleted {
            self.metrics.incr("conversations_deleted", 1).await;
        }
        Ok(deleted)
    }

    /// 解析链尾，供生成入口取上下文锚点。
    pub async fn resolve_tail(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<ConversationTail, ConversationError> {
        let conversation = self
            .conversations
            .get(tenant, id)
            .await?
            .ok_or(ConversationError::NotFound)?;
        Ok(match conversation.last_response_id {
            None => ConversationTail::Empty,
            Some(last) => ConversationTail::At(last),
        })
    }

    /// 读会话物化主快照（D30）。生成入口用它装配 LLM 上下文，`transcript` 用它渲染。
    pub async fn read_snapshot(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<ResolvedContext, ConversationError> {
        self.conversations.read_snapshot(tenant, id).await
    }

    /// 终态时把本轮 input+output 追加进会话快照（D30）。条目来自执行端最终产出，
    /// **不是**事件流回放（INV-48）。
    #[allow(clippy::too_many_arguments)]
    pub async fn append_turn(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        response_id: &ResponseId,
        input_items: Vec<ResponseItem>,
        output_items: Vec<ResponseItem>,
        reasoning: Option<String>,
        usage: Usage,
        status: ResponseStatus,
    ) -> Result<u64, ConversationError> {
        let now_ms = (self.now)();
        self.conversations
            .append_turn(
                tenant,
                id,
                response_id,
                input_items,
                output_items,
                reasoning,
                usage,
                status,
                now_ms,
            )
            .await
    }

    /// 轮次终态后推进链尾指针。**后写胜出**，见端口文档。
    pub async fn advance(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        last: &ResponseId,
    ) -> Result<(), ConversationError> {
        self.conversations.advance(tenant, id, last).await
    }

    /// 占用互斥标记并原子发出 `turn_started`（D28）。忙则返回 `Busy` 命名持有者。
    pub async fn acquire_active(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        response_id: &ResponseId,
    ) -> Result<u64, ConversationError> {
        let now_ms = (self.now)();
        self.conversations
            .acquire_active(tenant, id, response_id, now_ms)
            .await
    }

    /// 释放互斥标记并原子发出 `turn_completed`（D28）。条件释放、幂等。
    pub async fn release_active(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        response_id: &ResponseId,
        status: ResponseStatus,
    ) -> Result<u64, ConversationError> {
        let now_ms = (self.now)();
        self.conversations
            .release_active(tenant, id, response_id, status, now_ms)
            .await
    }

    /// 接管「持有者已终态」的残留标记，不发事件。
    pub async fn release_stale_active(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        holder: &ResponseId,
    ) -> Result<bool, ConversationError> {
        self.conversations
            .release_stale_active(tenant, id, holder)
            .await
    }

    /// 追加非轮次事件（business / response_deleted）。
    pub async fn append_event(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        kind: ConversationEventKind,
    ) -> Result<u64, ConversationError> {
        let now_ms = (self.now)();
        self.conversations.append_event(tenant, id, kind, now_ms).await
    }

    /// 读事件（排他游标），供 SSE 订阅复用。
    pub async fn read_after(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        starting_after: Option<u64>,
        limit: usize,
        wait_ms: u64,
    ) -> Result<Vec<ConversationEvent>, ConversationError> {
        self.conversations
            .read_after(tenant, id, starting_after, limit, wait_ms)
            .await
    }

    /// 列出该租户的全部会话容器，新在前。
    pub async fn list(&self, tenant: &TenantId) -> Result<Vec<Conversation>, ConversationError> {
        self.conversations.list(tenant).await
    }

    /// 一次取回容器的完整对话历史（D30）。
    ///
    /// 直接读会话物化主快照：它本就是完整历史的单一权威来源，一次读取即得全部。
    /// 这也是不实现官方 `items` 子资源的原因——那会把分页强加给每一个只想恢复
    /// 页面的调用方。
    pub async fn transcript(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<ResolvedContext, TranscriptError> {
        Ok(self.conversations.read_snapshot(tenant, id).await?)
    }
}
