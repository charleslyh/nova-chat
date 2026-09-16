//! Conversations 能力层：用例编排，无 axum 依赖。
//!
//! 会话是对话内容的**长期权威来源**（D30）：它存链尾指针、轮次锁、事件流，以及物化
//! 的主快照（每轮 delta 追加）。本层只做需要编排的事——注入时钟、记录计数、把「容器
//! 不存在」与「容器还没有轮次」分开。
//!
//! 纯转发方法（`advance`、`read_after`、`append_turn`）**不在这里**：它们只是把参数
//! 递给端口，多一层包装既不增加语义也不减少调用方需要知道的事，只是多一处要跟着端口
//! 一起改的代码。需要它们的调用方（执行端、SSE 骨架）直接依赖对应的端口 trait。

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::clock::Clock;
use crate::context::ResolvedContext;
use crate::conversation::{Conversation, ConversationEventKind, ConversationId};
use crate::identity::TenantId;
use crate::ports::{metric, ConversationError, ConversationStore, MetricsSink};
use crate::protocol::MetadataValue;
use crate::response::{ResponseId, ResponseStatus};

/// Conversations 用例编排。
pub struct ConversationsService {
    conversations: Arc<dyn ConversationStore>,
    clock: Arc<dyn Clock>,
    metrics: Arc<dyn MetricsSink>,
}

impl ConversationsService {
    pub fn new(
        conversations: Arc<dyn ConversationStore>,
        clock: Arc<dyn Clock>,
        metrics: Arc<dyn MetricsSink>,
    ) -> Self {
        Self {
            conversations,
            clock,
            metrics,
        }
    }

    pub async fn create(
        &self,
        tenant: &TenantId,
        metadata: BTreeMap<String, MetadataValue>,
    ) -> Result<Conversation, ConversationError> {
        // 不写前探活（D28）：库不可用由失败返回错误直接暴露，低概率失败用「治疗」而非
        // 「预防」。启动探活（fail-fast）仍在装配处。
        let conversation = Conversation::new(
            ConversationId::new(),
            tenant.clone(),
            metadata,
            self.clock.now_ms(),
        );
        let created = self.conversations.create(conversation).await?;
        self.metrics.incr(metric::CONVERSATIONS_CREATED, 1);
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
        metadata: BTreeMap<String, MetadataValue>,
    ) -> Result<Conversation, ConversationError> {
        self.conversations
            .update_metadata(tenant, id, metadata)
            .await
    }

    /// 删除容器。快照与事件流随之消失；响应**记录**不级联——这是既定决策而非悬案：
    /// 记录级删除（D24）与官方「Items in the conversation will not be deleted」一致，
    /// 移除的是指针，不抹除说过的话。
    pub async fn delete(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<bool, ConversationError> {
        let deleted = self.conversations.delete(tenant, id).await?;
        if deleted {
            self.metrics.incr(metric::CONVERSATIONS_DELETED, 1);
        }
        Ok(deleted)
    }

    pub async fn list(&self, tenant: &TenantId) -> Result<Vec<Conversation>, ConversationError> {
        self.conversations.list(tenant).await
    }

    /// 解析链尾，供生成入口取上下文锚点。
    ///
    /// 返回 `Option<ResponseId>`：容器不存在是 `Err(NotFound)`，所以 `None` 只可能是
    /// 「容器存在但还没有轮次」。曾经这里有一个两变体的枚举，理由是「要区分不存在与
    /// 首轮」——但那个区分本来就由错误分支承担，枚举没有表达任何 `Option` 没有的事。
    pub async fn resolve_tail(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<Option<ResponseId>, ConversationError> {
        let conversation = self
            .conversations
            .get(tenant, id)
            .await?
            .ok_or(ConversationError::NotFound)?;
        Ok(conversation.last_response_id)
    }

    /// 读会话物化主快照（D30）。
    ///
    /// 生成入口用它装配 LLM 上下文，transcript 用它渲染整段历史——一次读取即得全部，
    /// 这也是不实现官方 `items` 子资源的原因：那会把分页强加给每一个只想恢复页面的
    /// 调用方。
    pub async fn read_snapshot(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
    ) -> Result<ResolvedContext, ConversationError> {
        self.conversations.read_snapshot(tenant, id).await
    }

    /// 占用互斥标记并原子发出 `turn_started`（D28）。忙则返回 `Busy` 命名持有者。
    pub async fn acquire_active(
        &self,
        tenant: &TenantId,
        id: &ConversationId,
        response_id: &ResponseId,
    ) -> Result<u64, ConversationError> {
        self.conversations
            .acquire_active(tenant, id, response_id, self.clock.now_ms())
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
        self.conversations
            .release_active(tenant, id, response_id, status, self.clock.now_ms())
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
        self.conversations
            .append_event(tenant, id, kind, self.clock.now_ms())
            .await
    }
}
