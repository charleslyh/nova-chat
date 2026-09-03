---
name: service-layer-extraction
overview: 把 gateway 的「能力层 + HTTP 层 + 后台维护」抽成独立 library `nova-responses`，gateway 退化为「永远连真实后端（sql+redis）」的薄装配，mem 适配器与内嵌执行只进验证层（harness 的 mem-gateway 二进制）。这是 D25「能力层抽离」第二步（独立成 crate）的落地，也是 X 期 plan 架构图已画但未交付的部分。
---

## 用户需求

1. gateway 应只承担接入层/协议处理，`store_backend` 配置不应存在，gateway 永远使用真实后端（sql + redis）。
2. `nova-responses-service` 应独立封装（D25 决策原文「分两步，第二步独立成 crate」，被「视多协议复用」误置）。
3. mem 适配器只存在于验证层，通过「复用能力层库 + 注入端口」做测试。

## 核心决策

- **crate 边界**：新建 library `nova-responses`，承载「能力层（service）+ HTTP 层（routes/sse/auth/error/state）+ 后台维护（sweeper/shutdown/clock）+ config」。它只依赖 `nova-responses-core` 端口，**不依赖任何具体 adapter**（mem/sql/redis/agent 都不依赖）。
- **gateway 二进制**：`main.rs` 退化为薄装配，只连 `adapters-sql` + `adapters-event-log-redis`，调 `nova_responses::build_app`。删除 `store_backend` 分支与 `execution.rs`（不再内嵌执行）。
- **验证层**：`testing/harness` 新增 `[[bin]] nova-responses-mem-gateway`，连 mem + `adapters-completions-mock` + 内嵌执行，复用 `nova_responses::build_app`。L2 起的是它，不是生产 gateway。
- **命名**：library 名用 `nova-responses`（比 D25 的 `nova-responses-service` 更宽，因为它还含 HTTP 层与后台维护）。`nova-responses-core` 保持零依赖不合并。
- **Config**：整体平移进 `nova-responses`（阶段 1 不拆分，只删 `store_backend`）；`scheduler`/`exec_ttl_ms`/`max_concurrent_executions` 等 mem 验证装配字段在后续阶段收敛到 mem-gateway 侧。

## 分阶段

1. **Y1 建 library 并平移**：新建 `crates/nova-responses`，平移 gateway 的 `service/routes/sse/state/auth/error/clock/sweeper/shutdown/config`，`crate::` 引用不变，提供 `build_app(state) -> Router` 入口。gateway 暂时「空壳」（main.rs 委托 library），全量测试仍绿。
2. **Y2 gateway 退化**：main.rs 重写为薄装配（连 sql+redis），删 `store_backend` 分支、`execution.rs`、mem/completions-mock/agent 依赖。
3. **Y3 验证层 mem-gateway**：harness 新增 mem-gateway 二进制（连 mem + 内嵌执行 + echo/scripted），`xtask procs` 改起它。
4. **Y4 契约测试复用 library**：`http_contract.rs` 去掉 `#[path]` 重编译，改为依赖 `nova-responses` + mem。
5. **Y5 收口**：更新 `check-deps` 门禁（gateway 不得依赖 mem/agent；nova-responses 不得依赖任何 adapter）、workspace members、文档，全量验证。

## 目标目录结构

```
crates/nova-responses/          # [NEW] 能力层 + HTTP 层 + 后台维护（无具体 adapter 依赖）
  src/lib.rs                    # 模块声明 + build_app 入口 + pub use
  src/service/  src/routes/  src/sse.rs  src/state.rs
  src/auth.rs  src/error.rs  src/clock.rs  src/sweeper.rs  src/shutdown.rs  src/config.rs
crates/gateway/                 # [MODIFY] 只剩薄装配 main.rs
  src/main.rs                   # 连 sql+redis → build_app → 起 HTTP
testing/harness/                # [MODIFY] 新增 mem-gateway 二进制
  src/bin/mem-gateway.rs        # 连 mem + 内嵌执行 → build_app → 起 HTTP
```
