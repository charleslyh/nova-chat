# 当期计划 · 存储分工重构（responses 走事件流，conversation 存持久快照）

> 状态：**已完成**
> 依据：[D30](../architecture/decisions.md#d30-存储分工重构responses-走事件流conversation-存持久快照)
> 上一期（D29 Agent 结构解耦）成果见 §3

---

## 1. 交付项

| # | 交付 | 状态 |
|---|---|---|
| **1** | 移除 `ContextStore` 端口，职责拆分：快照读写并入 `ConversationStore`（`read_snapshot` / `append_turn`），response 检索重建并入 `ResponseEventLog`（回放流） | ✅ |
| **2** | `StoredResponse` → `ResponseRecord`（仅元数据，含 `SnapshotRef` 锚点，不再物化祖先快照）；`ClaimedResponse` 改带元数据 | ✅ |
| **3** | service 层重写：`create` 只写元数据 + 解析锚点校验上界；`retrieve` TTL 内回放重建；`transcript` 改读会话主快照 | ✅ |
| **4** | agent-runtime 编排：claim 后按锚点 `read_snapshot` 一次；终态 settle 改为 `complete + append_turn + advance/release_active`，`append_turn` 直接传 `AgentOutcome.items` | ✅ |
| **5** | mock 参考实现同步改造：MemWorld 会话主快照 + delta、`/rpc` 新操作、client 新端口桩 | ✅ |
| **6** | 验证层适配：L0 `output-provenance` 改为「销毁流后会话快照完整」；L1/L2 harness 驱动适配 | ✅ |
| **7** | 文档：`decisions.md` D30、`invariants.md` INV-34/48/54、新建 `07-storage-integration.md`、更新架构复验图与各设计文档 | ✅ |

## 2. 成果

- **存储 O(n²) → O(n)**：会话主快照 + 每轮 delta 取代 D24 每环物化全量快照。
- **与上游对齐**：responses 短命（TTL），conversation 是长期对话的 system of record。
- **原子性边界迁移**：INV-34 从「创建时 ledger+context 同事务」到「终态时 ledger.complete + append_turn 同事务」；INV-48 RESTATE（输出非回放派生）。
- **存储选型解耦**：领域层只交付 trait + mock 参考实现 + 接入要求文档，后端由外部接入方注入。
- 全部测试通过（`cargo test --workspace` 全绿、`check-deps` 通过、`coverage` 100%）。

## 3. 上一期（D29）成果

Agent 结构解耦（编排器 + AgentRunner + 模拟验证进程）已完成：

- **1** `nova-agent` → `nova-agent-runtime`，新增 `AgentRunner` trait + `AgentTask` / `AgentOutcome` / `AgentEventSink` 强类型契约。
- **2** 编排器 `AgentRuntime`（claim → 组装 → 提交）与 `EventSink` 分离；`new` / `start` 分离支持缩扩容。
- **3** completions 出站抽象（Scheduler / ToolExecutor / completions 类型）从 core 下沉到 `mock-agentd`。
- **4** 模拟验证进程 `mock-agentd` 移到 `verify/mock/`，装配 mem-server + `MockAgentRunner`（ReAct loop）+ `AgentRuntime`。
- **5** `ResponseRecord.tools` 改存 inbound 形状（单一数据源）；测试迁移。

更早的 Y/Z 期（能力层独立成 crate + 验证拓扑同构）与 X 期（D25 执行进程独立 + 在途缓冲共享化）成果见归档。
