# 当期计划 · Agent 结构重构（编排器 + AgentRunner + 模拟验证进程）

> 状态：**已完成**
> 依据：[D29](../architecture/decisions.md#d29-agent-结构解耦编排器--agentrunner--模拟验证进程)（Agent 结构解耦）
> 上一期（Y/Z 期）成果见 §3

---

## 1. 交付项

| # | 交付 | 状态 |
|---|---|---|
| **1** | crate `nova-agent` 改名 `nova-agent-runtime`，新增 `AgentRunner` trait + `AgentTask` / `AgentOutcome` / `AgentEventSink`（强类型契约，字段用 core 稳定类型） | ✅ |
| **2** | 拆分编排器：`AgentRuntime`（claim → 组装 → 提交）+ `EventSink`（事件 append）；`new` / `start` 分离以支持缩扩容 | ✅ |
| **3** | completions 出站抽象（`CompletionsRequestScheduler` / `CompletionsSink` / `ToolExecutor` 及 completions 类型）从 core 下沉到 `testing/agentd-mock` | ✅ |
| **4** | 模拟验证进程 `nova-agentd-mock` 移到 `testing/` 下，装配 mem-server + `MockAgentRunner`（React 循环）+ `AgentRuntime` | ✅ |
| **5** | `StoredResponse.tools` 改存 inbound 形状（单一数据源）；测试迁移（`engine_end_to_end` → `agent_runtime_e2e`，`http_contract` 改用 `AgentRuntime` + `MockAgentRunner`） | ✅ |

## 2. 成果

- **三层解耦**：进程壳（agentd-mock）只装配；编排器（`AgentRuntime`）负责 claim / 组装 / 提交；执行（`AgentRunner` 实现）负责 React 循环。换 provider / 换 agent SDK 不再改编排器。
- **completions 下沉**：core 只保留存储 / 领域 / 协议契约（`ResponseItem` / `EventBody` / `StoredResponse` / `protocol::Tool` / `RequestProvenance` 等）；completions 出站抽象归入验证进程，抽象与否由具体 runner 实现自决。
- **取消正确性 / 及时性分离**：fence（`attempt`）保证正确性（拒绝过期写入），sink `Stop` 单一传导取消及时性（流式场景几 ms 内传导并中断模型调用），heartbeat 仅保活、不承担取消通知。
- 生产 agentd 只预留 `AgentRunner` 抽象，本期不落地：未来对接真实 agent SDK（Moray）+ redis/mq。
- 全部测试通过（含 103 个 conformance 契约用例、18 个 agent_runtime_e2e、21 个 http_contract）。

## 3. 上一期（Y/Z 期）成果

Y/Z 期（能力层独立成 crate + 验证拓扑同构）已完成：

- **Y1** 新建 `nova-responses` library，平移 gateway 的能力层 + HTTP 层 + 后台维护。
- **Y2** gateway 后端改为编译期 feature 门控（mem / sql 互斥）。
- **Z 期** 验证拓扑与生产同构：mem 改为共享载体（`nova-responses-mem-server` + `adapters-mem-client`），执行 / 维护独立进程（`nova-agentd-mock` / `nova-responses-sweep`），L2 覆盖提升到 baseline 100%。

更早的 X 期（D25 执行进程独立 + 在途缓冲共享化）与 W 期（D20/21/22 子集重构）成果见归档。
