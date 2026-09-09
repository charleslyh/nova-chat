//! Responses 能力层：用例编排，无 axum 依赖。
//!
//! # 边界
//!
//! 输入领域请求 + 端口集合，输出领域结果。这里**不出现** axum 类型、`HeaderMap`、
//! HTTP 状态码映射——那些留在 `routes` 层。这样 responses 业务逻辑可用纯异步
//! 测试覆盖（注入 mem/sql adapter），无需启动 HTTP 服务（D25 ⑤）。
//!
//! 协议解析、严格校验、租户鉴权、节点路由/转发仍是接入层的职责；本层只编排
//! `resolve_context → create → append(Created)` 这条业务主线。
//!
//! # D30 数据流
//!
//! 创建只写**元数据**（锚点引用 + 本轮 input + 工具体声明），不再物化全量快照。
//! 长期历史在会话快照里（`ConversationStore::read_snapshot`），response 自身对象
//! 由事件流回放重建（TTL 内）。终态提交在编排层（`nova-agent-runtime`）完成：
//! `ledger.complete` + `conversation.append_turn`（INV-34 的原子性边界迁到这里）。

use std::sync::Arc;

use serde_json::Value;

use crate::protocol::{Tool, ToolChoice};
use crate::{
    response_object, AppendEvent, Attempt, ConversationError, ConversationEventKind,
    ConversationId, CreateOutcome, EventBody, EventLogError, IdempotencyKey, LedgerError,
    MetricsSink, ResolvedContext, ResponseEventKind, ResponseEventLog, ResponseId, ResponseItem,
    ResponseLedger, ResponseRecord, ResponseStatus, SnapshotRef, TenantId, Usage,
};

use crate::config::Config;
use crate::service::conversations::{ConversationTail, ConversationsService};

/// 能力层错误：包装各端口错误，由接入层映射为 HTTP 状态。
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error(transparent)]
    Ledger(#[from] LedgerError),
    #[error(transparent)]
    EventLog(#[from] EventLogError),
    #[error(transparent)]
    Conversation(#[from] ConversationError),
}

/// 创建结果。`ReadOnly` / `Overloaded` 不是错误，而是账本返回的正常拒绝。
pub enum CreateResult {
    /// 新建成功，`Created` 事件已发出。
    Accepted { record: ResponseRecord },
    /// 幂等重放，返回原生成（绝不产生第二个，FR-3）。
    Duplicate { existing: ResponseRecord },
    /// 降级只读。
    ReadOnly,
    /// 待领取/在途量达阈值。
    Overloaded,
}

/// 创建生成的领域意图。
///
/// 接入层已经把 wire 形状消解掉：`input` 简写已规范化为 [`ResponseItem`]，
/// `conversation` / `previous_response_id` 引用已解析为 [`SnapshotRef`] 锚点。传
/// 这个结构而不是「原始请求 + 并列的 `input_items`」，消除了二者必须一致的隐含契约。
#[derive(Debug, Clone)]
pub struct CreateIntent {
    pub model: String,
    pub instructions: Option<String>,
    pub store: bool,
    pub tools: Vec<Tool>,
    pub tool_choice: Option<ToolChoice>,
    pub input_items: Vec<ResponseItem>,
    pub source: SnapshotRef,
}

/// `ResponsesService` 的依赖集合。聚合为一个 struct 而不是六个并列参数，装配方
/// 按名组装，新增依赖不改签名。
pub struct ResponsesDeps {
    pub ledger: Arc<dyn ResponseLedger>,
    pub event_log: Arc<dyn ResponseEventLog>,
    pub conversations: Arc<ConversationsService>,
    pub now: Arc<dyn Fn() -> u64 + Send + Sync>,
    pub metrics: Arc<dyn MetricsSink>,
    pub cfg: Arc<Config>,
}

/// Responses 用例编排。
pub struct ResponsesService {
    ledger: Arc<dyn ResponseLedger>,
    event_log: Arc<dyn ResponseEventLog>,
    conversations: Arc<ConversationsService>,
    now: Arc<dyn Fn() -> u64 + Send + Sync>,
    metrics: Arc<dyn MetricsSink>,
    cfg: Arc<Config>,
}

impl ResponsesService {
    pub fn new(deps: ResponsesDeps) -> Self {
        Self {
            ledger: deps.ledger,
            event_log: deps.event_log,
            conversations: deps.conversations,
            now: deps.now,
            metrics: deps.metrics,
            cfg: deps.cfg,
        }
    }

    pub fn now_ms(&self) -> u64 {
        (self.now)()
    }

    /// 创建生成：解析锚点 → 校验上下文 → 取会话锁 → 写账本（仅元数据）→ 发 `Created`。
    ///
    /// 会话锁在这里取，而不是在接入层：这样无论调用方走标准 `/v1/responses` 还是
    /// 将来任何门面，`TurnStarted` 都不会漏发，接入层也只需做传输。
    pub async fn create(
        &self,
        tenant: &TenantId,
        intent: &CreateIntent,
        idempotency_key: Option<IdempotencyKey>,
    ) -> Result<CreateResult, ServiceError> {
        let now_ms = self.now_ms();

        // 会话标识收敛为锚点：容器存的就是链尾指针，所以下面只有一条装配路径。
        // `intent.source` 本身就是锚点（SnapshotRef），这里只需为 record 拆出
        // previous/conversation_id 两个归属字段。
        let (previous, conversation_id) = match &intent.source {
            SnapshotRef::Root => (None, None),
            SnapshotRef::Previous(id) => (Some(id.clone()), None),
            SnapshotRef::Conversation(id) => {
                let anchor = match self.conversations.resolve_tail(tenant, id).await? {
                    // 首轮：容器还没有链尾，空上下文。这与「容器不存在」不同，后者
                    // 已在 resolve_tail 内报 NotFound——绝不静默降级为空上下文，否则
                    // 打错 id 的调用方会拿到一个失忆的回复而无从察觉。
                    ConversationTail::Empty => None,
                    ConversationTail::At(last) => Some(last),
                };
                (anchor, Some(id.clone()))
            }
        };

        // 解析并校验前驱上下文（D30）：读会话快照校验深度/字节上界，断裂即失败，
        // 不留半创建记录。快照本身不复制进 record。
        let resolved = self.resolve_context(tenant, &intent.source).await?;
        if !matches!(intent.source, SnapshotRef::Root) {
            self.metrics.incr("chain_resolved_depth", resolved.depth as u64);
        }

        // 不写前探活（D28）：库不可用由失败返回错误直接暴露。准入失败由
        // `release_after_failed_admission` 补偿释放互斥标记。
        let response_id = ResponseId::new(self.cfg.node_tag.clone());

        // 准入闸门（D28）：conversation 的互斥标记。已有轮次在途即 Busy（接入层
        // 转 409）。失败时不写任何事件、不留半状态，这是端口契约的一部分。
        if let Some(conversation_id) = &conversation_id {
            self.acquire_turn(tenant, conversation_id, &response_id).await?;
        }

        // 缺省幂等键取 response_id，保证「同一生成仅一条记录」（FR-3）。
        let idempotency_key =
            idempotency_key.unwrap_or_else(|| IdempotencyKey(response_id.to_string()));
        let expires_at_ms = if intent.store {
            Some(now_ms.saturating_add(self.cfg.content_retention_ms))
        } else {
            None
        };

        let record = ResponseRecord {
            response_id: response_id.clone(),
            previous_response_id: previous,
            conversation_id,
            tenant_id: tenant.clone(),
            model: intent.model.clone(),
            // 仅用于检索回显，永不进入上下文（INV-49）。
            instructions: intent.instructions.clone(),
            // 本轮的工具体声明：由调用方 `tools` 参数逐请求声明（而非静态部署
            // 配置），原样落进 record（inbound 形状），provider 转换由执行端的
            // runner 内部完成（单一数据源：存调用方声明，不做双向转换）。
            tools: intent.tools.clone(),
            tool_choice: intent.tool_choice.clone(),
            input_items: intent.input_items.clone(),
            reasoning: None,
            status: ResponseStatus::Queued,
            usage: Usage::default(),
            created_at_ms: now_ms,
            completed_at_ms: None,
            stored: intent.store,
            expires_at_ms,
            integrity: None,
            idempotency_key: Some(idempotency_key.clone()),
            owner: None,
            attempt: Attempt::default(),
        };

        // 锁已在手，之后任何一条非「已受理」的出路都必须把它还回去，否则会话永久
        // 锁死。用一个内部函数把这些出路收在一处，就不必在每个 `?` 和每个分支上
        // 各写一遍补偿——漏掉一处的后果是不可自愈的。
        let outcome = self.admit(&record, idempotency_key, now_ms).await;

        match &outcome {
            Ok(CreateResult::Accepted { .. }) => {}
            _ => {
                if let Some(conversation_id) = &record.conversation_id {
                    self.release_after_failed_admission(tenant, conversation_id, &response_id)
                        .await;
                }
            }
        }
        outcome
    }

    /// 把锚点解析为可执行的继承上下文（D30）。只用于创建时的上界校验，快照不被
    /// 复制进 record——执行端按锚点再次读取会话快照。
    ///
    /// `Previous` 裸链（无会话锚点）沿账本反查其归属会话后读快照；无会话归属的裸链
    /// 在 D30 下没有持久载体，按 `ChainBroken` 处理（与快照物化前一致地显式失败）。
    async fn resolve_context(
        &self,
        tenant: &TenantId,
        anchor: &SnapshotRef,
    ) -> Result<ResolvedContext, ServiceError> {
        let resolved = match anchor {
            SnapshotRef::Root => ResolvedContext::default(),
            SnapshotRef::Conversation(id) => self.conversations.read_snapshot(tenant, id).await?,
            SnapshotRef::Previous(id) => {
                let record = self
                    .ledger
                    .get(id)
                    .await?
                    .ok_or_else(|| ConversationError::ChainBroken(id.clone()))?;
                if !record.is_referencable_by(tenant) {
                    if &record.tenant_id != tenant {
                        return Err(ConversationError::CrossTenant.into());
                    }
                    return Err(ConversationError::NotStored.into());
                }
                match record.anchor() {
                    SnapshotRef::Conversation(cid) => {
                        self.conversations.read_snapshot(tenant, &cid).await?
                    }
                    // A bare response (no conversation) holds no durable snapshot:
                    // reconstruct its own input+output from the event stream (TTL).
                    SnapshotRef::Root => self.reconstruct_bare(&record).await?,
                    // A bare chain pointing at another bare response has no durable
                    // home for the deeper history — reported as broken, not silently
                    // truncated.
                    SnapshotRef::Previous(_) => {
                        return Err(ConversationError::ChainBroken(id.clone()).into());
                    }
                }
            }
        };

        // 上界校验在创建前失败，绝不静默截断（INV-41）。
        if resolved.depth >= self.cfg.chain_limits.max_depth {
            return Err(ConversationError::ChainTooLong {
                limit: self.cfg.chain_limits.max_depth,
            }
            .into());
        }
        if resolved.bytes > self.cfg.chain_limits.max_bytes {
            return Err(ConversationError::ChainTooLarge {
                limit: self.cfg.chain_limits.max_bytes,
            }
            .into());
        }
        Ok(resolved)
    }

    /// 取轮次互斥标记，必要时接管一个「持有者已终态」的残留标记（D28）。
    async fn acquire_turn(
        &self,
        tenant: &TenantId,
        conversation_id: &ConversationId,
        response_id: &ResponseId,
    ) -> Result<(), ServiceError> {
        let holder = match self
            .conversations
            .acquire_active(tenant, conversation_id, response_id)
            .await
        {
            Ok(_) => return Ok(()),
            Err(ConversationError::Busy { holder }) => holder,
            Err(e) => return Err(e.into()),
        };

        let holder_status = self
            .ledger
            .get(&holder)
            .await
            .ok()
            .flatten()
            .map(|record| record.status);
        let stale = holder_status.map(|s| s.is_terminal()).unwrap_or(true);
        if !stale {
            return Err(ConversationError::Busy { holder }.into());
        }

        let released = self
            .conversations
            .release_stale_active(tenant, conversation_id, &holder)
            .await?;
        tracing::warn!(
            conversation_id = %conversation_id,
            stale_holder = %holder,
            released,
            "took over a turn marker whose holder had already reached a terminal state"
        );
        self.metrics.incr("conversation_stale_locks_released", 1);

        self.conversations
            .acquire_active(tenant, conversation_id, response_id)
            .await?;
        Ok(())
    }

    /// 写账本（仅元数据）→ 发 `Created`。
    async fn admit(
        &self,
        record: &ResponseRecord,
        idempotency_key: IdempotencyKey,
        now_ms: u64,
    ) -> Result<CreateResult, ServiceError> {
        match self
            .ledger
            .create(record.clone(), idempotency_key, now_ms)
            .await?
        {
            CreateOutcome::Accepted { .. } => {
                // 首事件，让立即订阅者看到确定起点。**携带完整 response 对象**
                // （含 input），这正是「B 端在轮次进行中加入也能补齐全部内容」所依赖
                // 的既有行为，不可回退为只带 id。创建时无输出。
                let created = AppendEvent::lifecycle(
                    record.response_id.clone(),
                    ResponseEventKind::Created,
                    response_object(record, &[]),
                );
                self.event_log.append(created).await?;

                self.metrics.incr("responses_created", 1);

                Ok(CreateResult::Accepted {
                    record: record.clone(),
                })
            }
            CreateOutcome::Duplicate { response_id } => {
                // 幂等重放返回原生成，不产生第二个。
                let existing = self
                    .ledger
                    .get(&response_id)
                    .await?
                    .ok_or(LedgerError::NotFound)?;
                Ok(CreateResult::Duplicate { existing })
            }
            CreateOutcome::ReadOnly => Ok(CreateResult::ReadOnly),
            CreateOutcome::Overloaded => Ok(CreateResult::Overloaded),
        }
    }

    /// 受理失败后归还轮次互斥标记（D28）。
    async fn release_after_failed_admission(
        &self,
        tenant: &TenantId,
        conversation_id: &ConversationId,
        response_id: &ResponseId,
    ) {
        if let Err(err) = self
            .conversations
            .release_active(tenant, conversation_id, response_id, ResponseStatus::Failed)
            .await
        {
            tracing::warn!(
                conversation_id = %conversation_id,
                response_id = %response_id,
                error = %err,
                "failed to release the turn marker after admission failed; \
                 the conversation stays busy until a terminal path releases it"
            );
        }
    }

    /// 查询生成的当前状态对象（未完成返回 `in_progress` 部分对象，不失败）。
    ///
    /// 由事件流回放重建：账本确认存在与租户归属，最新一条生命周期事件的
    /// `response` 载荷即当前对象（含终态 output，D30）。流已过期时账本仍可能命中，
    /// 但事件回放为空——此时退化为元数据渲染（无 output）。
    pub async fn retrieve(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
    ) -> Result<Option<Value>, ServiceError> {
        match self.ledger.get(response_id).await? {
            None => Ok(None),
            Some(record) if &record.tenant_id != tenant => Ok(None),
            Some(record) => match self.latest_response_value(response_id).await? {
                Some(value) => Ok(Some(value)),
                None => Ok(Some(response_object(&record, &[]))),
            },
        }
    }

    /// 同步模式：等终态事件，超时返回当前状态对象供轮询。
    ///
    /// 超时不是生成失败——调用方可转轮询。租户归属在调用前已由 `create`/`retrieve`
    /// 路径确认（事件流按 response id 寻址，本身无租户维度），故这里不再接收 tenant。
    pub async fn wait_terminal(
        &self,
        response_id: &ResponseId,
        fallback: &ResponseRecord,
    ) -> Result<Value, ServiceError> {
        let budget_ms = self.cfg.sync_wait_timeout_ms;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(budget_ms);
        let mut cursor: Option<u64> = None;

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match self
                .event_log
                .read_after(response_id, cursor, 256, remaining.as_millis() as u64)
                .await
            {
                Ok(batch) if !batch.is_empty() => {
                    let terminal = batch.iter().any(|e| e.kind.is_terminal());
                    cursor = batch.last().map(|e| e.sequence_number).or(cursor);
                    if terminal {
                        break;
                    }
                }
                Ok(_) => break,
                Err(e) => return Err(ServiceError::EventLog(e)),
            }
        }

        match self.latest_response_value(response_id).await? {
            Some(value) => Ok(value),
            None => Ok(response_object(fallback, &[])),
        }
    }

    /// 取消在途生成：终态化 + 记录部分用量由 ledger 完成，此处发终态事件并关流。
    pub async fn cancel(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
    ) -> Result<Option<Value>, ServiceError> {
        let now_ms = self.now_ms();

        self.ledger.cancel(tenant, response_id, now_ms).await?;

        let record = self.ledger.get(response_id).await.ok().flatten();

        // Cancellation is a terminal path like any other, and the engine will
        // **not** reach its own: the ledger is already terminal, so its next
        // `complete` is refused as a stale transition and the engine returns
        // early. Releasing here is therefore not belt-and-braces — it is the only
        // release this response gets.
        //
        // The tail is deliberately not advanced: a cancelled turn committed no
        // output, so advancing would leave the conversation ending on an
        // unanswered question.
        if let Some(record) = &record {
            if let Some(conversation_id) = &record.conversation_id {
                if let Err(err) = self
                    .conversations
                    .release_active(tenant, conversation_id, response_id, ResponseStatus::Cancelled)
                    .await
                {
                    tracing::warn!(
                        conversation_id = %conversation_id,
                        response_id = %response_id,
                        error = %err,
                        "could not release the turn marker after cancelling; the \
                         conversation stays busy until the reap path releases it"
                    );
                }
            }
        }

        let response = record
            .as_ref()
            .map(|r| response_object(r, &[]))
            .unwrap_or_else(|| {
                serde_json::json!({
                    "id": response_id.to_string(),
                    "object": "response",
                    "status": "cancelled",
                })
            });
        let _ = self
            .event_log
            .append(AppendEvent::lifecycle(
                response_id.clone(),
                ResponseEventKind::Failed,
                response.clone(),
            ))
            .await;
        let _ = self
            .event_log
            .close(response_id, now_ms, self.cfg.retain_after_terminal_ms)
            .await;

        self.metrics.incr("responses_cancelled", 1);

        Ok(Some(response))
    }

    /// 删除单条记录（记录级，D30）。返回是否确有记录被删。
    ///
    /// 删的是账本记录与事件流；会话快照里继承的副本**原样保留**（沿用 D24 语义，
    /// 即「从对话移除」而非「从继承它的快照抹除」）。若该响应属于某会话，广播
    /// `ResponseDeleted` 供各端移除气泡。
    pub async fn delete(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
    ) -> Result<bool, ServiceError> {
        // 先读关联，再删：删掉之后就再也拿不到 conversation_id 了。
        let conversation_id = self
            .ledger
            .get(response_id)
            .await
            .ok()
            .flatten()
            .and_then(|record| record.conversation_id);

        let deleted = self.ledger.delete(response_id).await?;
        let _ = self.event_log.remove(response_id).await;

        if deleted {
            self.metrics.incr("responses_deleted", 1);

            if let Some(conversation_id) = &conversation_id {
                if let Err(err) = self
                    .conversations
                    .append_event(
                        tenant,
                        conversation_id,
                        ConversationEventKind::ResponseDeleted {
                            response_id: response_id.clone(),
                        },
                    )
                    .await
                {
                    tracing::warn!(
                        conversation_id = %conversation_id,
                        response_id = %response_id,
                        error = %err,
                        "could not announce the deletion on the conversation stream; \
                         other devices will notice on their next transcript read"
                    );
                }
            }
        }
        Ok(deleted)
    }

    /// 重建一个「裸链」response（无会话锚点）的 input+output（D30）。
    ///
    /// input 在账本记录里；output 只在事件流里（终态事件携带完整对象），故从流
    /// 回放取出。仅用于 `previous_response_id` 无会话的兼容路径——它的持久性受
    /// 事件流 TTL 约束，过期即 `ChainBroken`。
    async fn reconstruct_bare(
        &self,
        record: &ResponseRecord,
    ) -> Result<ResolvedContext, ServiceError> {
        let mut items = record.input_items.clone();
        let mut bytes = items.iter().map(ResponseItem::byte_len).sum::<usize>();
        if let Some(value) = self.latest_response_value(&record.response_id).await? {
            if let Ok(output) =
                serde_json::from_value::<Vec<ResponseItem>>(value.get("output").cloned().unwrap_or_default())
            {
                bytes += output.iter().map(ResponseItem::byte_len).sum::<usize>();
                items.extend(output);
            }
        }
        Ok(ResolvedContext {
            items,
            reasoning: Vec::new(),
            depth: 1,
            bytes,
        })
    }

    /// 回放事件流，取最新一条生命周期事件的 `response` 载荷（D30 检索重建）。
    async fn latest_response_value(
        &self,
        response_id: &ResponseId,
    ) -> Result<Option<Value>, ServiceError> {
        let mut cursor: Option<u64> = None;
        let mut latest: Option<Value> = None;
        loop {
            let batch = self.event_log.read_after(response_id, cursor, 256, 0).await?;
            if batch.is_empty() {
                break;
            }
            let terminal = batch.iter().any(|e| e.kind.is_terminal());
            cursor = batch.last().map(|e| e.sequence_number);
            for event in &batch {
                if let EventBody::Response { response } = &event.body {
                    latest = Some(response.clone());
                }
            }
            if terminal {
                break;
            }
        }
        Ok(latest)
    }
}
