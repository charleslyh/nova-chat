//! Sessions 能力层：用例编排，无 axum 依赖（D26）。
//!
//! 会话层只管「状态如何广播」：事件流、轮次锁、业务事件。对话内容一律不经此层，
//! 唯一真相源是响应链的物化快照（D24）。
//!
//! 容器相关的一切都委托给 [`ConversationsService`]，而不是再持一份
//! `ConversationStore`。否则「如何新建容器」「如何解析链尾」会有两份实现，两份
//! 实现必然在其中一份改动时分叉。

use std::sync::Arc;

use nova_responses_core::{
    Clock, ConversationError, ConversationId, MetricsSink, ResolvedContext, ResponseId,
    ResponseStatus, Session, SessionError, SessionEvent, SessionEventKind, SessionId, SessionStore,
    TenantId,
};
use serde_json::Value;

use crate::service::conversations::{ConversationsService, TranscriptError};

/// 会话层错误：会话端口错误 + 编排容器时可能的容器侧错误。
///
/// 两者不合并：容器不可用与会话不可用是不同的降级面，接入层需要据此给出不同的
/// 诊断信息，压成一个会让运维看不出该修哪一边。
#[derive(Debug, thiserror::Error)]
pub enum SessionsServiceError {
    #[error(transparent)]
    Session(#[from] SessionError),
    #[error(transparent)]
    Conversation(#[from] ConversationError),
    #[error(transparent)]
    Transcript(#[from] TranscriptError),
}

/// Sessions 用例编排。
pub struct SessionsService {
    sessions: Arc<dyn SessionStore>,
    conversations: Arc<ConversationsService>,
    clock: Arc<dyn Clock>,
    metrics: Arc<dyn MetricsSink>,
}

impl SessionsService {
    pub fn new(
        sessions: Arc<dyn SessionStore>,
        conversations: Arc<ConversationsService>,
        clock: Arc<dyn Clock>,
        metrics: Arc<dyn MetricsSink>,
    ) -> Self {
        Self {
            sessions,
            conversations,
            clock,
            metrics,
        }
    }

    /// 创建会话。
    ///
    /// `conversation` 为 `None` 时新建一个容器，为 `Some` 时绑定既有容器——后者
    /// 让先用官方 SDK 起步、之后才需要多端投递的调用方不必丢弃已有历史。
    pub async fn create(
        &self,
        tenant: &TenantId,
        conversation: Option<ConversationId>,
    ) -> Result<Session, SessionsServiceError> {
        // 会话库写前探活：不可用即拒写，绝不留下「有容器无会话」的半状态
        // （INV-46）。容器侧的探活在 ConversationsService 内部完成。
        self.sessions.health().await?;

        let conversation_id = match conversation {
            Some(id) => {
                // 绑定前校验归属：跨租户一律读作不存在（SEC-2）。
                self.conversations
                    .retrieve(tenant, &id)
                    .await?
                    .ok_or(ConversationError::NotFound)?;
                id
            }
            None => {
                self.conversations
                    .create(tenant, Default::default())
                    .await?
                    .id
            }
        };

        let now_ms = self.clock.now_ms().await;
        let session = self
            .sessions
            .create(
                Session::new(SessionId::new(), tenant.clone(), conversation_id, now_ms),
                now_ms,
            )
            .await?;
        self.metrics.incr("sessions_created", 1).await;
        Ok(session)
    }

    pub async fn retrieve(
        &self,
        tenant: &TenantId,
        id: &SessionId,
    ) -> Result<Option<Session>, SessionError> {
        self.sessions.get(tenant, id).await
    }

    /// 列出该租户的全部会话，新在前。
    ///
    /// 排序契约落在端口（`SessionStore::list`）而非这里：UI 直接渲染即可，不必再
    /// 排一次。列表是「状态广播」这层的能力——它回答「有哪些会话可订阅」，与
    /// 「某会话里聊了什么」分属两个问题。
    pub async fn list(&self, tenant: &TenantId) -> Result<Vec<Session>, SessionError> {
        self.sessions.list(tenant).await
    }

    /// 拥有该容器的会话（绑定互斥，至多一个）。
    ///
    /// 生成入口靠它把标准的 `POST /v1/responses { conversation }` 接到会话锁上，
    /// 从而不必引入任何官方没有的请求字段。
    pub async fn find_by_conversation(
        &self,
        tenant: &TenantId,
        conversation: &ConversationId,
    ) -> Result<Option<Session>, SessionError> {
        self.sessions.get_by_conversation(tenant, conversation).await
    }

    /// 删除会话与其事件流。
    ///
    /// **不删除关联容器**：容器承载对话历史，会话只承载广播状态。删掉会话就连带
    /// 删掉历史，会让「关掉这个页面」变成「销毁这段对话」。
    pub async fn delete(
        &self,
        tenant: &TenantId,
        id: &SessionId,
    ) -> Result<bool, SessionError> {
        self.sessions.health().await?;
        let deleted = self.sessions.delete(tenant, id).await?;
        if deleted {
            self.metrics.incr("sessions_deleted", 1).await;
        }
        Ok(deleted)
    }

    /// 追加业务事件，与对话事件同处一个序号空间，严格保序。
    ///
    /// 业务事件**不进入模型上下文**：上下文装配只读响应链，从不读事件流。
    pub async fn append_business_event(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        kind: String,
        payload: Value,
    ) -> Result<u64, SessionError> {
        self.sessions.health().await?;
        let now_ms = self.clock.now_ms().await;
        let seq = self
            .sessions
            .append_event(
                tenant,
                id,
                SessionEventKind::Business { kind, payload },
                now_ms,
            )
            .await?;
        self.metrics.incr("session_business_events", 1).await;
        Ok(seq)
    }

    /// 取轮次锁并原子发出 `TurnStarted`。已占用返回 `Busy`（接入层转 409）。
    pub async fn begin_turn(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        response_id: &ResponseId,
    ) -> Result<u64, SessionError> {
        let now_ms = self.clock.now_ms().await;
        self.sessions
            .begin_turn(tenant, id, response_id, now_ms)
            .await
    }

    /// 接管一个「持有者已终态」的残留锁。不发事件——终态事件已由完成方发过。
    ///
    /// 判断持有者是否已终态是调用方的职责（它才拿得到账本），本层只做转发：把
    /// 「是否可接管」的判据放进来会让会话层依赖账本，而它不需要知道账本存在。
    pub async fn release_stale_lock(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        holder: &ResponseId,
    ) -> Result<bool, SessionError> {
        self.sessions.release_stale_lock(tenant, id, holder).await
    }

    /// 释放轮次锁并原子发出 `TurnCompleted`。所有终态路径都必须走到这里。
    pub async fn end_turn(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        response_id: &ResponseId,
        status: ResponseStatus,
    ) -> Result<u64, SessionError> {
        let now_ms = self.clock.now_ms().await;
        self.sessions
            .end_turn(tenant, id, response_id, status, now_ms)
            .await
    }

    /// 广播「某条响应记录已删除」，各端据此移除气泡。
    pub async fn note_response_deleted(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        response_id: &ResponseId,
    ) -> Result<u64, SessionError> {
        let now_ms = self.clock.now_ms().await;
        self.sessions
            .append_event(
                tenant,
                id,
                SessionEventKind::ResponseDeleted {
                    response_id: response_id.clone(),
                },
                now_ms,
            )
            .await
    }

    /// 读事件（排他游标）。订阅走 SSE 骨架，这里供非流式读取复用。
    pub async fn read_after(
        &self,
        tenant: &TenantId,
        id: &SessionId,
        starting_after: Option<u64>,
        limit: usize,
        wait_ms: u64,
    ) -> Result<Vec<SessionEvent>, SessionError> {
        self.sessions
            .read_after(tenant, id, starting_after, limit, wait_ms)
            .await
    }

    /// 一次取回会话的完整对话历史，供重开页面渲染气泡。
    ///
    /// 与 `read_after` 合起来就是完整的页面恢复：历史给内容，事件流给状态与业务
    /// 事件。两者都不需要额外的渲染快照机制。
    pub async fn transcript(
        &self,
        tenant: &TenantId,
        id: &SessionId,
    ) -> Result<Option<ResolvedContext>, SessionsServiceError> {
        let Some(session) = self.sessions.get(tenant, id).await? else {
            return Ok(None);
        };
        Ok(Some(
            self.conversations
                .transcript(tenant, &session.conversation_id)
                .await?,
        ))
    }
}
