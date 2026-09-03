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
    Attempt, Clock, ContextError, ContextStore, CreateOutcome, EventLogError, IdempotencyKey,
    LedgerError, MetricsSink, ResponseEvent, ResponseEventKind, ResponseEventLog, ResponseId,
    ResponseItem, ResponseLedger, ResponseStatus, StoredResponse, TenantId, Usage,
};

use crate::config::Config;

/// 能力层错误：包装三类端口错误，由接入层映射为 HTTP 状态。
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error(transparent)]
    Context(#[from] ContextError),
    #[error(transparent)]
    Ledger(#[from] LedgerError),
    #[error(transparent)]
    EventLog(#[from] EventLogError),
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
    clock: Arc<dyn Clock>,
    metrics: Arc<dyn MetricsSink>,
    cfg: Arc<Config>,
}

impl ResponsesService {
    pub fn new(
        ledger: Arc<dyn ResponseLedger>,
        event_log: Arc<dyn ResponseEventLog>,
        context: Arc<dyn ContextStore>,
        clock: Arc<dyn Clock>,
        metrics: Arc<dyn MetricsSink>,
        cfg: Arc<Config>,
    ) -> Self {
        Self {
            ledger,
            event_log,
            context,
            clock,
            metrics,
            cfg,
        }
    }

    pub async fn now_ms(&self) -> u64 {
        self.clock.now_ms().await
    }

    /// 创建生成：解析前驱 → 固化快照 → 写账本 → 写内容 → 发 `Created` 事件。
    ///
    /// `previous` 是已解析的前驱 id（接入层已在链亲和路由前解析）；`input_items`
    /// 是接入层经协议子集校验后的输入条目。
    pub async fn create(
        &self,
        tenant: &TenantId,
        request: &CreateResponseRequest,
        input_items: Vec<ResponseItem>,
        previous: Option<ResponseId>,
        idempotency_key: Option<IdempotencyKey>,
    ) -> Result<CreateResult, ServiceError> {
        let now_ms = self.now_ms().await;

        // 解析前驱历史（D24）：在创建前固化扁平快照，断裂即失败，不留半创建记录。
        let mut snapshot: Vec<ResponseItem> = Vec::new();
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
            snapshot_depth = resolved.depth;
        }

        // 存储开启时先探活：库不可用拒写而非静默不存（INV-46）。
        if request.store {
            self.context.health().await?;
        }

        let response_id = ResponseId::new(self.cfg.node_tag.clone());
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
            tenant_id: tenant.clone(),
            model: request.model.clone(),
            // 仅用于检索回显，永不进入链（INV-49）。
            instructions: request.instructions.clone(),
            input_items,
            output_items: Vec::new(),
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
            context_depth: snapshot_depth,
        };

        match self
            .ledger
            .create(record.clone(), idempotency_key, now_ms)
            .await?
        {
            CreateOutcome::Accepted { .. } => {
                if request.store {
                    self.context.put(record.clone()).await?;
                }

                // 首事件，让立即订阅者看到确定起点。
                let created = ResponseEvent::lifecycle(
                    response_id,
                    ResponseEventKind::Created,
                    record.to_response_value(),
                );
                self.event_log.append(created).await?;

                self.metrics.incr("responses_created", 1).await;

                Ok(CreateResult::Accepted { record })
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
        let now_ms = self.now_ms().await;

        self.ledger.cancel(tenant, response_id, now_ms).await?;

        let record = self.context.get(tenant, response_id).await.ok().flatten();

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
            .append(ResponseEvent::lifecycle(
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
    pub async fn delete(
        &self,
        tenant: &TenantId,
        response_id: &ResponseId,
    ) -> Result<bool, ServiceError> {
        let deleted = self.context.delete(tenant, response_id).await?;
        if deleted {
            self.metrics.incr("responses_deleted", 1).await;
        }
        Ok(deleted)
    }
}
