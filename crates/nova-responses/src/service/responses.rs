//! Responses 能力层：用例编排，无 axum 依赖。
//!
//! # 边界
//!
//! 输入领域请求 + 端口集合，输出领域结果。这里**不出现** axum 类型、`HeaderMap`、
//! HTTP 状态码映射——那些留在 `routes` 层。这样 responses 业务逻辑可用纯异步
//! 测试覆盖（注入 mem/sql adapter），无需启动 HTTP 服务（D25 ⑤）。
//!
//! 协议解析、严格校验、租户鉴权、节点路由/转发仍是接入层的职责；本层只编排
//! `resolve_chain → create → put → append(Created)` 这条业务主线。

use std::sync::Arc;

use nova_responses_core::protocol::CreateResponseRequest;
use nova_responses_core::{
    AppendEvent, Attempt, ContextError, ContextStore, ConversationError,
    ConversationEventKind, ConversationId, CreateOutcome, EventLogError, IdempotencyKey,
    LedgerError, MetricsSink, ResponseEventKind, ResponseEventLog, ResponseId, ResponseItem,
    ResponseLedger, ResponseStatus, StoredResponse, TenantId, Usage,
};

use crate::config::Config;
use crate::service::conversations::{ConversationTail, ConversationsService};

/// 能力层错误：包装各端口错误，由接入层映射为 HTTP 状态。
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error(transparent)]
    Context(#[from] ContextError),
    #[error(transparent)]
    Ledger(#[from] LedgerError),
    #[error(transparent)]
    EventLog(#[from] EventLogError),
    #[error(transparent)]
    Conversation(#[from] ConversationError),
}

/// 本次生成继承哪份上下文。
///
/// 三个变体最终**都收敛到同一条装配路径**：解析出一个锚点 `ResponseId`，交给既有
/// `resolve_chain` 读物化快照（D24）。`Conversation` 不是第二种上下文来源，只是
/// 第二种指定锚点的方式——容器存的就是链尾指针。所以这里没有"两来源归一"的分支
/// 逻辑，`resolve_chain` 仍是唯一入口。
#[derive(Debug, Clone, PartialEq)]
pub enum ContextSource {
    /// 无前驱，空上下文。
    Fresh,
    /// 由 `previous_response_id` 直接指定锚点。
    Previous(ResponseId),
    /// 由 `conversation` 指定：先取容器链尾，再当作锚点用。
    Conversation(ConversationId),
}

/// 创建结果。`ReadOnly` / `Overloaded` 不是错误，而是账本返回的正常拒绝。
pub enum CreateResult {
    /// 新建成功，`Created` 事件已发出。
    Accepted { record: StoredResponse },
    /// 幂等重放，返回原生成（绝不产生第二个，FR-3）。
    Duplicate { existing: StoredResponse },
    /// 降级只读。
    ReadOnly,
    /// 待领取/在途量达阈值。
    Overloaded,
}

/// Responses 用例编排。
pub struct ResponsesService {
    ledger: Arc<dyn ResponseLedger>,
    event_log: Arc<dyn ResponseEventLog>,
    context: Arc<dyn ContextStore>,
    conversations: Arc<ConversationsService>,
    now: Arc<dyn Fn() -> u64 + Send + Sync>,
    metrics: Arc<dyn MetricsSink>,
    cfg: Arc<Config>,
}

impl ResponsesService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ledger: Arc<dyn ResponseLedger>,
        event_log: Arc<dyn ResponseEventLog>,
        context: Arc<dyn ContextStore>,
        conversations: Arc<ConversationsService>,
        now: Arc<dyn Fn() -> u64 + Send + Sync>,
        metrics: Arc<dyn MetricsSink>,
        cfg: Arc<Config>,
    ) -> Self {
        Self {
            ledger,
            event_log,
            context,
            conversations,
            now,
            metrics,
            cfg,
        }
    }

    pub fn now_ms(&self) -> u64 {
        (self.now)()
    }

    /// 创建生成：解析锚点 → 固化快照 → 取会话锁 → 写账本 → 写内容 → 发 `Created`。
    ///
    /// `source` 是上下文来源（三个变体最终都收敛到 `resolve_chain`）；`input_items`
    /// 是接入层经协议子集校验后的输入条目。
    ///
    /// 会话锁在这里取，而不是在接入层：这样无论调用方走标准 `/v1/responses` 还是
    /// 将来任何门面，`TurnStarted` 都不会漏发，接入层也只需做传输。
    pub async fn create(
        &self,
        tenant: &TenantId,
        request: &CreateResponseRequest,
        input_items: Vec<ResponseItem>,
        source: ContextSource,
        idempotency_key: Option<IdempotencyKey>,
    ) -> Result<CreateResult, ServiceError> {
        let now_ms = self.now_ms();

        // 会话标识收敛为锚点：容器存的就是链尾指针，所以下面只有一条装配路径。
        let (previous, conversation_id) = match source {
            ContextSource::Fresh => (None, None),
            ContextSource::Previous(id) => (Some(id), None),
            ContextSource::Conversation(id) => {
                let anchor = match self.conversations.resolve_tail(tenant, &id).await? {
                    // 首轮：容器还没有链尾，空上下文。这与「容器不存在」不同，后者
                    // 已在 resolve_tail 内报 NotFound——绝不静默降级为空上下文，否则
                    // 打错 id 的调用方会拿到一个失忆的回复而无从察觉。
                    ConversationTail::Empty => None,
                    ConversationTail::At(last) => Some(last),
                };
                (anchor, Some(id))
            }
        };

        // 解析前驱历史（D24）：在创建前固化扁平快照，断裂即失败，不留半创建记录。
        let mut snapshot: Vec<ResponseItem> = Vec::new();
        let mut snapshot_reasoning: Vec<Option<String>> = Vec::new();
        let mut snapshot_depth: usize = 0;
        if let Some(previous) = &previous {
            let resolved = self
                .context
                .resolve_chain(tenant, previous, self.cfg.chain_limits)
                .await?;
            self.metrics
                .incr("chain_resolved_depth", resolved.depth as u64)
                .await;
            snapshot = resolved.items;
            // 祖先的 reasoning 随快照一起物化，用于渲染；不进模型上下文。
            snapshot_reasoning = resolved.reasoning;
            snapshot_depth = resolved.depth;
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
        let expires_at_ms = if request.store {
            Some(now_ms.saturating_add(self.cfg.content_retention_ms))
        } else {
            None
        };

        let record = StoredResponse {
            response_id: response_id.clone(),
            previous_response_id: previous,
            conversation_id,
            tenant_id: tenant.clone(),
            model: request.model.clone(),
            // 仅用于检索回显，永不进入链（INV-49）。
            instructions: request.instructions.clone(),
            // 本轮的工具体声明：由调用方 `tools` 参数逐请求声明（而非静态部署
            // 配置），原样落进 record（inbound 形状），provider 转换由执行端的
            // runner 内部完成（单一数据源：存调用方声明，不做双向转换）。
            tools: request.tools.clone().unwrap_or_default(),
            tool_choice: request.tool_choice.clone(),
            input_items,
            output_items: Vec::new(),
            reasoning: None,
            status: ResponseStatus::Queued,
            usage: Usage::default(),
            created_at_ms: now_ms,
            completed_at_ms: None,
            stored: request.store,
            expires_at_ms,
            integrity: None,
            integrity_alg: None,
            node_tag: self.cfg.node_tag.clone(),
            idempotency_key: Some(idempotency_key.clone()),
            owner: None,
            attempt: Attempt::default(),
            context: snapshot,
            context_reasoning: snapshot_reasoning,
            context_depth: snapshot_depth,
        };

        // 锁已在手，之后任何一条非「已受理」的出路都必须把它还回去，否则会话永久
        // 锁死。用一个内部函数把这些出路收在一处，就不必在每个 `?` 和每个分支上
        // 各写一遍补偿——漏掉一处的后果是不可自愈的。
        let outcome = self
            .admit(tenant, request, &record, &response_id, idempotency_key, now_ms)
            .await;

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

    /// 取轮次互斥标记，必要时接管一个「持有者已终态」的残留标记（D28）。
    ///
    /// 标记可能比持有者活得更久：持有者进程可能在账本终态迁移与释放标记之间被杀，
    /// 或者释放调用本身失败（reap 路径尤其如此——它释放失败后不会再被选中重试）。
    /// 若不处理，该会话将永久 409。
    ///
    /// 判据用「持有者是否已终态」而非超时：账本知道确切答案，直接问它。
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

        // 账本读不到（记录已被删除或清理）同样视为可接管：既然没有记录，就不可能
        // 还有生成在跑。
        let holder_status = self
            .ledger
            .get(&holder)
            .await
            .ok()
            .flatten()
            .map(|record| record.status);
        let stale = holder_status.map(|s| s.is_terminal()).unwrap_or(true);
        if !stale {
            // 真正在途：明确拒绝，绝不排队等它。
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
        self.metrics.incr("conversation_stale_locks_released", 1).await;

        // 只重试一次。再次 Busy 说明有另一个调用方刚抢到标记，那是真冲突而非残留。
        self.conversations
            .acquire_active(tenant, conversation_id, response_id)
            .await?;
        Ok(())
    }

    /// 写账本 → 写内容 → 发 `Created`。
    #[allow(clippy::too_many_arguments)]
    async fn admit(
        &self,
        tenant: &TenantId,
        request: &CreateResponseRequest,
        record: &StoredResponse,
        response_id: &ResponseId,
        idempotency_key: IdempotencyKey,
        now_ms: u64,
    ) -> Result<CreateResult, ServiceError> {
        match self
            .ledger
            .create(record.clone(), idempotency_key, now_ms)
            .await?
        {
            CreateOutcome::Accepted { .. } => {
                if request.store {
                    self.context.put(record.clone()).await?;
                }

                // 首事件，让立即订阅者看到确定起点。**携带完整 response 对象**
                // （含 input），这正是「B 端在轮次进行中加入也能补齐全部内容」所依赖
                // 的既有行为，不可回退为只带 id。
                let created = AppendEvent::lifecycle(
                    response_id.clone(),
                    ResponseEventKind::Created,
                    record.to_response_value(),
                );
                self.event_log.append(created).await?;

                self.metrics.incr("responses_created", 1).await;

                Ok(CreateResult::Accepted {
                    record: record.clone(),
                })
            }
            CreateOutcome::Duplicate { response_id } => {
                // 幂等重放返回原生成，不产生第二个。
                let existing = self
                    .context
                    .get(tenant, &response_id)
                    .await?
                    .ok_or(ContextError::NotFound)?;
                Ok(CreateResult::Duplicate { existing })
            }
            CreateOutcome::ReadOnly => Ok(CreateResult::ReadOnly),
            CreateOutcome::Overloaded => Ok(CreateResult::Overloaded),
        }
    }

    /// 受理失败后归还轮次互斥标记（D28）。
    ///
    /// 归还失败只记日志、不改变调用方看到的结果：调用方要知道的是它的请求没有被
    /// 受理，而标记的残留是服务端问题。残留本身也不是永久的——引擎侧的终态路径与
    /// 回收路径都会再次释放。
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
    pub async fn retrieve(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
    ) -> Result<Option<StoredResponse>, ServiceError> {
        Ok(self.context.get(tenant, response_id).await?)
    }

    /// 同步模式：等终态事件，超时返回当前状态对象供轮询。
    ///
    /// 超时不是生成失败——调用方可转轮询。
    pub async fn wait_terminal(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
        fallback: &StoredResponse,
    ) -> Result<StoredResponse, ServiceError> {
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

        match self.context.get(tenant, response_id).await? {
            Some(record) => Ok(record),
            // store=false 无可读回对象，回退到已持有的内存 record。
            None => Ok(fallback.clone()),
        }
    }

    /// 取消在途生成：终态化 + 记录部分用量由 ledger 完成，此处发终态事件并关流。
    pub async fn cancel(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
    ) -> Result<Option<StoredResponse>, ServiceError> {
        let now_ms = self.now_ms();

        self.ledger.cancel(tenant, response_id, now_ms).await?;

        let record = self.context.get(tenant, response_id).await.ok().flatten();

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
                    // Logged, not propagated: the caller asked to cancel and the
                    // cancellation happened. Reporting a bookkeeping failure
                    // instead would suggest it did not.
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
            .map(|r| r.to_response_value())
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
                response,
            ))
            .await;
        let _ = self
            .event_log
            .close(response_id, now_ms, self.cfg.retain_after_terminal_ms)
            .await;

        self.metrics.incr("responses_cancelled", 1).await;

        Ok(record)
    }

    /// 删除单条已存内容（记录级，D24）。返回是否确有记录被删。
    ///
    /// 若该响应属于某会话，删除后向事件流广播一条 `ResponseDeleted`，各端据此移除
    /// 对应气泡——否则「A 端删掉了，B 端还显示着」，而 B 端无从知晓。
    pub async fn delete(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
    ) -> Result<bool, ServiceError> {
        // 先读关联，再删：删掉之后就再也拿不到 conversation_id 了。
        let conversation_id = self
            .context
            .get(tenant, response_id)
            .await
            .ok()
            .flatten()
            .and_then(|record| record.conversation_id);

        let deleted = self.context.delete(tenant, response_id).await?;
        if deleted {
            self.metrics.incr("responses_deleted", 1).await;

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
                    // 只记日志：记录确实已删除，用一个广播失败去否认它会更糟。代价是
                    // 其它端要等到下次拉取历史才会发现，而不是立刻。
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
}
