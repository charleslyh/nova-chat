//! Responses 能力层：用例编排，无 axum 依赖。
//!
//! # 边界
//!
//! 输入领域值 + 端口集合，输出领域结果。这里**不出现** axum 类型、`HeaderMap`、HTTP
//! 状态码映射——那些留在接入层。协议解析、严格校验、租户鉴权同样是接入层的职责；本层
//! 只编排 `resolve_context → create → append(Created)` 这条业务主线。
//!
//! 返回的是 [`ResponseObject`]（协议对象）而不是 `serde_json::Value`：调用方要渲染的
//! 东西必须有结构，否则「哪些字段存在」这件事就只能靠读实现来知道。
//!
//! # D30 数据流
//!
//! 创建只写**元数据**（锚点引用 + 本轮 input + 工具声明），不再物化全量快照。长期历史
//! 在会话快照里，response 自身对象由事件流回放重建（TTL 内）。终态提交在执行端
//! （`nova-agent-runtime`）完成：`ledger.complete` + `append_turn`。

use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use crate::clock::Clock;
use crate::config::Config;
use crate::context::ResolvedContext;
use crate::conversation::{ConversationEventKind, ConversationId};
use crate::events::AppendEvent;
use crate::identity::{IdempotencyKey, TenantId};
use crate::ports::{
    metric, ConversationError, CreateOutcome, MetricsSink, ResponseEventLog, ResponseLedger,
    TurnLock,
};
use crate::protocol::{ResponseItem, ResponseObject};
use crate::response::{
    ContextAnchor, ResponseId, ResponseRecord, ResponseStatus, TurnSpec,
};
use crate::service::conversations::ConversationsService;
use crate::service::error::{ContextError, ServiceError};

/// `ResponsesService` 的依赖集合。
///
/// 聚合为一个 struct 而不是六个并列参数，装配方按名组装，新增依赖不改签名。服务**持有
/// 它本身**而不是把六个字段再抄一遍——两个同构的 struct 加一段逐字段搬运，是同一件事
/// 的两份写法。
pub struct ResponsesDeps {
    pub ledger: Arc<dyn ResponseLedger>,
    pub event_log: Arc<dyn ResponseEventLog>,
    pub conversations: Arc<ConversationsService>,
    /// The turn lock, for the sweeper's reap path: a reaped turn is a terminal
    /// transition, so it owes the conversation a marker release (D28). Held directly
    /// rather than reached through `conversations` because the sweeper needs only this
    /// facet, not the whole conversation store.
    pub turn_lock: Arc<dyn TurnLock>,
    pub clock: Arc<dyn Clock>,
    pub metrics: Arc<dyn MetricsSink>,
    pub cfg: Arc<Config>,
}

/// Responses 用例编排。
pub struct ResponsesService {
    deps: ResponsesDeps,
    /// Sweeper shutdown sender: `Some` between [`Self::start`] and [`Self::stop`].
    /// Dropping it closes the channel, which the sweeper loop observes and exits on.
    sweeper: Mutex<Option<watch::Sender<()>>>,
}

impl ResponsesService {
    pub fn new(deps: ResponsesDeps) -> Self {
        Self {
            deps,
            sweeper: Mutex::new(None),
        }
    }

    /// Start the service's background work — currently the sweeper, which reaps lost
    /// claims and releases expired event buffers. Idempotent: a second call while
    /// running is a no-op.
    ///
    /// The loop is owned by this service rather than the assembly layer because reaping
    /// a lost claim is a response-lifecycle transition — the same lifecycle this service
    /// otherwise orchestrates (`create`, `cancel`).
    pub fn start(&self) {
        let mut guard = self.sweeper.lock().expect("sweeper lock poisoned");
        if guard.is_some() {
            return;
        }
        let (tx, rx) = watch::channel(());
        super::sweep::spawn(rx, &self.deps);
        *guard = Some(tx);
    }

    /// Stop the service's background work. Idempotent, and a stopped service can be
    /// restarted with [`Self::start`]. The sweeper exits at its next tick boundary —
    /// an in-flight tick is never interrupted.
    pub fn stop(&self) {
        // Dropping the sender closes the channel; `watch::Receiver::changed` then fails
        // and the loop breaks.
        self.sweeper.lock().expect("sweeper lock poisoned").take();
    }

    fn now_ms(&self) -> u64 {
        self.deps.clock.now_ms()
    }

    /// 创建生成：解析锚点 → 校验上下文 → 取会话锁 → 写账本（仅元数据）→ 发 `Created`。
    ///
    /// 会话锁在这里取，而不是在接入层：这样无论调用方走标准 `/v1/responses` 还是将来
    /// 任何门面，`TurnStarted` 都不会漏发，接入层也只需做传输。
    pub async fn create(
        &self,
        tenant: &TenantId,
        spec: TurnSpec,
        idempotency_key: Option<IdempotencyKey>,
    ) -> Result<CreateOutcome, ServiceError> {
        let now_ms = self.now_ms();

        // 解析并校验前驱上下文（D30）：读会话快照校验深度/条数/字节上界，断裂即失败，
        // 不留半创建记录。快照本身不复制进 record——执行端按锚点自己再读一次，所以这里
        // 只要「能装配且在界内」这个结论。
        let _ = self.resolve_context(tenant, &spec.anchor).await?;
        if !spec.anchor.is_root() {
            self.deps.metrics.incr(metric::CHAIN_RESOLVED, 1);
        }

        // 会话首轮时容器还没有链尾，锚点保持 Conversation 不变：归属由锚点单独表达，
        // 不需要再往 record 里塞一个 previous 副本。
        let conversation_id = spec.anchor.conversation_id().cloned();

        // 不写前探活（D28）：库不可用由失败返回错误直接暴露。准入失败由
        // `release_after_failed_admission` 补偿释放互斥标记。
        let response_id = ResponseId::new(self.deps.cfg.node_tag.clone());

        // 准入闸门（D28）：conversation 的互斥标记。已有轮次在途即 Busy（接入层转
        // 409）。失败时不写任何事件、不留半状态，这是端口契约的一部分。
        if let Some(conversation_id) = &conversation_id {
            self.acquire_turn(tenant, conversation_id, &response_id)
                .await?;
        }

        // 缺省幂等键取 response_id，保证「同一生成仅一条记录」（FR-3）。
        let idempotency_key =
            idempotency_key.unwrap_or_else(|| IdempotencyKey::for_response(&response_id));

        let record = ResponseRecord::queued(
            response_id.clone(),
            tenant.clone(),
            spec,
            idempotency_key.clone(),
            now_ms,
            self.deps.cfg.content_retention_ms,
        );

        // 锁已在手，之后任何一条非「已受理」的出路都必须把它还回去，否则会话永久锁死。
        // 用一个内部函数把这些出路收在一处，就不必在每个 `?` 和每个分支上各写一遍补偿
        // ——漏掉一处的后果是不可自愈的。
        let outcome = self.admit(record, idempotency_key, now_ms).await;

        if !matches!(outcome, Ok(CreateOutcome::Accepted(_))) {
            if let Some(conversation_id) = &conversation_id {
                self.release_after_failed_admission(tenant, conversation_id, &response_id)
                    .await;
            }
        }
        outcome
    }

    /// 把锚点解析为可执行的继承上下文（D30）。只用于创建时的上界校验，快照不被复制进
    /// record——执行端按锚点再次读取会话快照。
    ///
    /// `Previous` 裸链（无会话锚点）沿账本反查其归属会话后读快照；两级裸链在 D30 下没有
    /// 持久载体，按 `ChainBroken` 处理（显式失败，不静默截断）。
    async fn resolve_context(
        &self,
        tenant: &TenantId,
        anchor: &ContextAnchor,
    ) -> Result<ResolvedContext, ServiceError> {
        let resolved = match anchor {
            ContextAnchor::Root => ResolvedContext::default(),
            ContextAnchor::Conversation(id) => {
                self.deps.conversations.read_snapshot(tenant, id).await?
            }
            ContextAnchor::Previous(id) => {
                let record = self
                    .deps
                    .ledger
                    .get(id)
                    .await?
                    .ok_or_else(|| ContextError::ChainBroken(id.clone()))?;
                if !record.is_referencable_by(tenant) {
                    return Err(if &record.tenant_id != tenant {
                        ContextError::CrossTenant.into()
                    } else {
                        ContextError::NotStored.into()
                    });
                }
                match record.anchor() {
                    ContextAnchor::Conversation(cid) => {
                        self.deps.conversations.read_snapshot(tenant, cid).await?
                    }
                    // A bare response (no conversation) holds no durable snapshot:
                    // reconstruct its own input+output from the event stream (TTL).
                    ContextAnchor::Root => self.reconstruct_bare(&record).await?,
                    // A bare chain pointing at another bare response has no durable
                    // home for the deeper history — reported as broken, not silently
                    // truncated.
                    ContextAnchor::Previous(_) => {
                        return Err(ContextError::ChainBroken(id.clone()).into())
                    }
                }
            }
        };

        // 上界校验在创建前失败，绝不静默截断（INV-41）。
        let limits = &self.deps.cfg.chain;
        if resolved.turns >= limits.max_depth {
            return Err(ContextError::TooDeep {
                limit: limits.max_depth,
            }
            .into());
        }
        if resolved.item_count() > limits.max_items {
            return Err(ContextError::TooManyItems {
                limit: limits.max_items,
            }
            .into());
        }
        if resolved.bytes() > limits.max_bytes {
            return Err(ContextError::TooLarge {
                limit: limits.max_bytes,
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
            .deps
            .conversations
            .acquire_active(tenant, conversation_id, response_id)
            .await
        {
            Ok(_) => return Ok(()),
            Err(ConversationError::Busy { holder }) => holder,
            Err(e) => return Err(e.into()),
        };

        let holder_status = self
            .deps
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
            .deps
            .conversations
            .release_stale_active(tenant, conversation_id, &holder)
            .await?;
        tracing::warn!(
            conversation_id = %conversation_id,
            stale_holder = %holder,
            released,
            "took over a turn marker whose holder had already reached a terminal state"
        );
        self.deps
            .metrics
            .incr(metric::CONVERSATION_STALE_LOCKS_RELEASED, 1);

        self.deps
            .conversations
            .acquire_active(tenant, conversation_id, response_id)
            .await?;
        Ok(())
    }

    /// 写账本（仅元数据）→ 发 `Created`。
    async fn admit(
        &self,
        record: ResponseRecord,
        idempotency_key: IdempotencyKey,
        now_ms: u64,
    ) -> Result<CreateOutcome, ServiceError> {
        let outcome = self
            .deps
            .ledger
            .create(record, idempotency_key, now_ms)
            .await?;

        if let CreateOutcome::Accepted(record) = &outcome {
            // 首事件，让立即订阅者看到确定起点。**携带完整 response 对象**（含 input），
            // 这正是「B 端在轮次进行中加入也能补齐全部内容」所依赖的既有行为，不可回退
            // 为只带 id。创建时无输出。
            self.deps
                .event_log
                .append(AppendEvent::lifecycle(
                    record.response_id.clone(),
                    crate::events::ResponseEventKind::Created,
                    ResponseObject::without_output(record),
                ))
                .await?;
            self.deps.metrics.incr(metric::RESPONSES_CREATED, 1);
        }
        Ok(outcome)
    }

    /// 受理失败后归还轮次互斥标记（D28）。
    async fn release_after_failed_admission(
        &self,
        tenant: &TenantId,
        conversation_id: &ConversationId,
        response_id: &ResponseId,
    ) {
        if let Err(err) = self
            .deps
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

    /// 查询生成的当前状态对象（未完成返回部分对象，不失败）。
    ///
    /// 由事件流回放重建：账本确认存在与租户归属，最新一条生命周期事件的 `response`
    /// 载荷即当前对象（含终态 output，D30）。流已过期时账本仍可能命中，但事件回放为空
    /// ——此时退化为元数据渲染（无 output）。
    pub async fn retrieve(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
    ) -> Result<Option<ResponseObject>, ServiceError> {
        let Some(record) = self.deps.ledger.get(response_id).await? else {
            return Ok(None);
        };
        // 跨租户与不存在返回同一结果，id 无法被探测（SEC-2）。
        if &record.tenant_id != tenant {
            return Ok(None);
        }
        Ok(Some(self.current_object(&record).await?))
    }

    /// 同步模式：等终态事件，超时返回当前状态对象供轮询。
    ///
    /// 超时不是生成失败——调用方可转轮询。参数只有 record：response id 就在它里面，两个
    /// 参数并列时「必须彼此对应」这件事只能靠调用方记得，传错了没有任何东西会发现。
    pub async fn wait_terminal(
        &self,
        record: &ResponseRecord,
    ) -> Result<ResponseObject, ServiceError> {
        let response_id = &record.response_id;
        let deadline = tokio::time::Instant::now() + self.deps.cfg.sync_wait();
        let mut cursor: Option<u64> = None;

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let batch = self
                .deps
                .event_log
                .read_after(response_id, cursor, self.deps.cfg.event_page, remaining)
                .await?;
            if batch.is_empty() {
                break;
            }
            let terminal = batch.iter().any(|e| e.kind().is_terminal());
            cursor = batch.last().map(|e| e.sequence_number()).or(cursor);
            if terminal {
                break;
            }
        }

        self.current_object(record).await
    }

    /// 取消在途生成：终态化 + 记录部分用量由 ledger 完成，此处发终态事件并关流。
    ///
    /// 返回值不是 `Option`：取消要么失败（`Err`），要么产出一个对象。曾经的 `Option` 从
    /// 无 `None` 分支，只是逼每个调用方写一段永不执行的代码。
    pub async fn cancel(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
    ) -> Result<ResponseObject, ServiceError> {
        let now_ms = self.now_ms();

        self.deps.ledger.cancel(tenant, response_id, now_ms).await?;

        let record = self.deps.ledger.get(response_id).await.ok().flatten();

        // Cancellation is a terminal path like any other, and the engine will **not**
        // reach its own: the ledger is already terminal, so its next `complete` is
        // refused as a stale transition and the engine returns early. Releasing here
        // is therefore not belt-and-braces — it is the only release this response
        // gets.
        //
        // The tail is deliberately not advanced: a cancelled turn committed no output,
        // so advancing would leave the conversation ending on an unanswered question.
        if let Some(conversation_id) = record.as_ref().and_then(|r| r.conversation_id()) {
            if let Err(err) = self
                .deps
                .conversations
                .release_active(
                    tenant,
                    conversation_id,
                    response_id,
                    ResponseStatus::Cancelled,
                )
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

        let response = match &record {
            Some(record) => ResponseObject::without_output(record),
            // The record vanished between cancelling and reading it back. The stream
            // still has to end, and a stub is all the terminal event needs.
            None => ResponseObject::terminal_stub(response_id, ResponseStatus::Cancelled),
        };
        let _ = self
            .deps
            .event_log
            .append(AppendEvent::lifecycle(
                response_id.clone(),
                crate::events::ResponseEventKind::Failed,
                response.clone(),
            ))
            .await;
        let _ = self
            .deps
            .event_log
            .close(
                response_id,
                now_ms,
                self.deps.cfg.retain_after_terminal(),
            )
            .await;

        self.deps.metrics.incr(metric::RESPONSES_CANCELLED, 1);

        Ok(response)
    }

    /// 删除单条记录（记录级，D30）。返回是否确有记录被删。
    ///
    /// 删的是账本记录与事件流；会话快照里继承的副本**原样保留**（沿用 D24 语义，即
    /// 「从对话移除」而非「从继承它的快照抹除」）。若该响应属于某会话，广播
    /// `ResponseDeleted` 供各端移除气泡。
    pub async fn delete(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
    ) -> Result<bool, ServiceError> {
        // 先读关联，再删：删掉之后就再也拿不到会话归属了。
        let conversation_id = self
            .deps
            .ledger
            .get(response_id)
            .await
            .ok()
            .flatten()
            .and_then(|record| record.conversation_id().cloned());

        let deleted = self.deps.ledger.delete(response_id).await?;
        let _ = self.deps.event_log.remove(response_id).await;

        if deleted {
            self.deps.metrics.incr(metric::RESPONSES_DELETED, 1);

            if let Some(conversation_id) = &conversation_id {
                if let Err(err) = self
                    .deps
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

    /// 该响应此刻的对象：能从流里重建就用流里的（含 output），否则退化为元数据渲染。
    async fn current_object(
        &self,
        record: &ResponseRecord,
    ) -> Result<ResponseObject, ServiceError> {
        Ok(self
            .latest_response_object(&record.response_id)
            .await?
            .unwrap_or_else(|| ResponseObject::without_output(record)))
    }

    /// 重建一个「裸链」response（无会话锚点）的 input+output（D30）。
    ///
    /// input 在账本记录里；output 只在事件流里（终态事件携带完整对象），故从流回放取出
    /// ——从**类型化**的 `output` 字段，而不是按字符串键去翻渲染出来的 JSON。仅用于
    /// `previous_response_id` 无会话的兼容路径，其持久性受事件流 TTL 约束。
    async fn reconstruct_bare(
        &self,
        record: &ResponseRecord,
    ) -> Result<ResolvedContext, ServiceError> {
        let mut items: Vec<ResponseItem> = record.spec.input_items.clone();
        if let Some(object) = self.latest_response_object(&record.response_id).await? {
            items.extend(object.output);
        }
        Ok(ResolvedContext::from_items(items, 1))
    }

    /// 回放事件流，取最新一条生命周期事件的 `response` 载荷（D30 检索重建）。
    async fn latest_response_object(
        &self,
        response_id: &ResponseId,
    ) -> Result<Option<ResponseObject>, ServiceError> {
        let mut cursor: Option<u64> = None;
        let mut latest: Option<ResponseObject> = None;
        loop {
            let batch = match self
                .deps
                .event_log
                .read_after(
                    response_id,
                    cursor,
                    self.deps.cfg.event_page,
                    std::time::Duration::ZERO,
                )
                .await
            {
                Ok(batch) => batch,
                // 流已过期/已回收：不是失败，只是无法从流里重建（INV-40 无恢复路径）。
                Err(crate::ports::EventLogError::Unknown)
                | Err(crate::ports::EventLogError::Expired) => return Ok(latest),
                Err(e) => return Err(e.into()),
            };
            if batch.is_empty() {
                break;
            }
            let terminal = batch.iter().any(|e| e.kind().is_terminal());
            cursor = batch.last().map(|e| e.sequence_number());
            for event in &batch {
                if let Some(object) = event.response_object() {
                    latest = Some(object.clone());
                }
            }
            if terminal {
                break;
            }
        }
        Ok(latest)
    }
}
