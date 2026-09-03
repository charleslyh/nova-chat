# 当期计划 · Y/Z 期（能力层独立成 crate + 验证拓扑同构）

> 状态：**已完成**
> 依据：[D25](../architecture/decisions.md#d25-执行进程独立与在途缓冲共享化能力层抽离)（「分两步」的第二步 + 验证拓扑同构）
> 上一期（X 期）成果见 §3

---

## 1. 交付项

| # | 交付 | 状态 |
|---|---|---|
| **Y1** | 新建 `nova-responses` library，平移 gateway 的能力层 + HTTP 层 + 后台维护，`crate::` 引用不变 | ✅ |
| **Y2** | gateway 后端改为**编译期 feature 门控**：`mem`（默认，内嵌执行）与 `sql`（`--no-default-features --features sql`，执行 = agentd）互斥，后端依赖全部 `optional` | ✅ |
| **Y3** | `http_contract.rs` 从 gateway 移入 `nova-responses` 测试，去 `#[path]` 重编译，复用 library | ✅ |
| **Y4** | `check-deps` 新增「服务层无 adapter」「gateway 后端依赖 optional」门禁；workspace / justfile / 文档收口 | ✅ |

## 2. 成果

- **能力层独立**：`nova-responses` 承载用例编排 + HTTP 接入 + 后台维护，只依赖 `nova-responses-core` 端口，不依赖任何具体 adapter——D25 决策里「分两步，第二步独立成 crate」的落地。
- **后端编译期门控**：gateway 的 `mount()` 按 `#[cfg(feature)]` 静态分派，`mem` 与 `sql` 互斥（`compile_error!`），没有 `store_backend` 运行时配置。默认 `cargo build` 得到 mem 形态（协议兼容验证、本地开发、L2 都不需要数据库）；`just release` 用 `--no-default-features --features sql` 构建生产形态，release 二进制静态排除 mem / agent / completions-mock。
- **门禁守边界**：`check-deps` 拒绝 `nova-responses` 依赖任何 adapter、强制 gateway 的后端依赖 `optional = true`（防止某个后端泄漏进所有构建）。

### Z 期（验证拓扑同构）

Y 期的 mem 形态仍把执行内嵌在 gateway（`execution.rs`），与生产（独立 `nova-agentd`）进程拓扑不一致——验证测不到进程隔离、跨进程 claim、崩溃恢复等路径。Z 期落地了**验证拓扑与生产同构**：

- **mem 改为共享载体**：`nova-responses-mem-server`（数据面 `/rpc` + 控制面 `/control/*`）持有数据本体，`adapters-mem-client` 是实现了同样端口的 RPC 桩。gateway / agentd / sweep 各自持有一份客户端，连同一个 mem-server——与「gateway/agentd 都是 Postgres/Redis 的 client」完全同构。
- **执行与维护独立进程**：mem 形态摘掉内嵌执行（`execution.rs` 删除），统一走独立 `nova-agentd`；维护走独立 `nova-responses-sweep`（mem/sql 双 feature）。
- **数据面 / 控制面分离**：数据面走端口 RPC（生产路径），控制面（`unavailable` / `tamper` / `advance_clock`）是测试控制器独有的故障注入面。
- **L2 覆盖提升到 baseline 100%**：共享载体让之前只能 defer 到 L3 的性质（FR-11/14/30/31 跨节点订阅、FR-32 无粘性续订、FR-34 优雅停机）全部在 L2 落地。

## 3. 上一期（X 期）成果

X 期（D25 执行进程独立 + 在途缓冲共享化）已完成：能力层抽离第一步（gateway 内 service 模块）、`adapters-event-log-redis`、`nova-agentd`、claim 全局化、gateway 拆薄、验证体系更新。本期的 `nova-responses` 独立 crate 即其「第二步」。

更早的 W 期（D20/21/22 子集重构）成果与缺陷清单见归档。
