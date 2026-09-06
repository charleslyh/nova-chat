//! Conversations 能力层：用例编排，无 axum 依赖（D27）。
//!
//! 会话容器只是「指向响应链尾的指针」。本层负责 CRUD 编排与写前探活，以及把
//! 会话标识解析为链尾 `ResponseId`——这是 `/v1/responses` 携带 `conversation`
//! 时唯一需要的解析步骤，之后上下文装配仍走既有 `resolve_chain`（D24 零改动）。

use std::collections::BTreeMap;
use std::sync::Arc;

use nova_responses_core::{
    Clock, ContextError, ContextStore, Conversation, ConversationError, ConversationId,
    ConversationStore, MetricsSink, ResolvedContext, ResponseId, TenantId,
};

use crate::config::Config;

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
///
/// 容器端口与内容端口的错误分开保留：容器不存在是调用方问题，内容库不可用是服务
/// 端降级，接入层要据此给出不同状态码。
#[derive(Debug, thiserror::Error)]
pub enum TranscriptError {
    #[error(transparent)]
    Conversation(#[from] ConversationError),
    #[error(transparent)]
    Context(#[from] ContextError),
}

/// Conversations 用例编排。
pub struct ConversationsService {
    conversations: Arc<dyn ConversationStore>,
    /// 读对话历史用：容器只存链尾指针，内容在响应链的物化快照里（D24）。
    context: Arc<dyn ContextStore>,
    clock: Arc<dyn Clock>,
    metrics: Arc<dyn MetricsSink>,
    cfg: Arc<Config>,
}

impl ConversationsService {
    pub fn new(
        conversations: Arc<dyn ConversationStore>,
        context: Arc<dyn ContextStore>,
        clock: Arc<dyn Clock>,
        metrics: Arc<dyn MetricsSink>,
        cfg: Arc<Config>,
    ) -> Self {
        Self {
            conversations,
            context,
            clock,
            metrics,
            cfg,
        }
    }

    pub async fn create(
        &self,
        tenant: &TenantId,
        metadata: BTreeMap<String, String>,
    ) -> Result<Conversation, ConversationError> {
        // 写前探活：库不可用拒写而非静默不存（INV-46）。
        self.conversations.health().await?;
        let now_ms = self.clock.now_ms().await;
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
        self.conversations.health().await?;
        self.conversations
            .update_metadata(tenant, id, metadata)
            .await
    }

    /// 删除容器。**不级联删除响应记录**——与 D24 的记录级删除一致，也与官方
    /// 「Items in the conversation will not be deleted」一致。
    pub async fn delete(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<bool, ConversationError> {
        self.conversations.health().await?;
        let deleted = self.conversations.delete(tenant, id).await?;
        if deleted {
            self.metrics.incr("conversations_deleted", 1).await;
        }
        Ok(deleted)
    }

    /// 解析链尾，供生成入口取上下文锚点。
    ///
    /// 容器不存在返回 `NotFound`，由接入层翻译；绝不静默降级为空上下文——那会让
    /// 打错 id 的调用方拿到一个「失忆」的回复而无从察觉。
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

    /// 轮次终态后推进链尾指针。**后写胜出**，见端口文档。
    pub async fn advance(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        last: &ResponseId,
    ) -> Result<(), ConversationError> {
        self.conversations.advance(tenant, id, last).await
    }

    /// 一次取回容器的完整对话历史。
    ///
    /// 用 `resolve_chain` 而非分页遍历：链尾响应已携带全部祖先条目的扁平副本
    /// （D24），所以「完整历史」本来就是一次读取。这也是不实现官方 `items`
    /// 子资源的原因——那会把分页强加给每一个只想恢复页面的调用方。
    ///
    /// 沿用 `chain_limits` 是安全的而非将就：任何一轮生成都在创建时受同一组上界
    /// 约束，超限的链根本不可能被创建出来，所以这里不会因为上界而读不全。
    pub async fn transcript(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<ResolvedContext, TranscriptError> {
        match self.resolve_tail(tenant, id).await? {
            // 空容器返回空历史而非报错：刚建好的会话是正常状态。
            ConversationTail::Empty => Ok(ResolvedContext::default()),
            ConversationTail::At(last) => Ok(self
                .context
                .resolve_chain(tenant, &last, self.cfg.chain_limits)
                .await?),
        }
    }
}
