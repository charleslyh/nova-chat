---
name: review-fixes-schedule-cancel-and-conformance
overview: 执行复审发现的五项修正：P1 修复 sweep 归档顺序（先快照后释放锁）、P2 让 LLM 调用（scheduler.schedule）也竞速取消、T3 配套测试、T1 conformance 补幂等与 input-only 契约断言、T2 harness 的 Cancel/Reap 步骤补归档以保持 L1 保真度。
todos:
  - id: p1-sweep-order
    content: sweep.rs：归档块移到 release_active 之前，对齐「先快照后释放锁」
    status: completed
  - id: p2-schedule-race
    content: mock_runner.rs：schedule 调用用 select 竞速 cancel.cancelled() 返回 Superseded
    status: completed
  - id: t3-hang-test
    content: 改 hang 测试：断言 runner 自行返回 Executed::Superseded，去掉 abort()
    status: completed
    dependencies:
      - p2-schedule-race
  - id: t1-conformance
    content: conformance 补幂等与 input-only 契约断言，并调整 delete 断言计数
    status: completed
  - id: t2-harness-archive
    content: harness 的 Cancel/Reap 步骤补归档（本地实现回放 helper，先归档后释放）
    status: completed
  - id: verify-all
    content: 全量 cargo test --workspace 与 read_lints 验证
    status: completed
    dependencies:
      - p1-sweep-order
      - p2-schedule-race
      - t3-hang-test
      - t1-conformance
      - t2-harness-archive
---

## 用户需求

对已实现的「协作式取消 + 未完成轮次归档」两项能力进行复审后，用户确认执行审查报告发现的五项修正（按 P1 → P2 → T3 → T1 → T2 顺序）。

## 修正项概览

- **P1（高）**：sweep 的 reap 归档顺序违反「先快照后释放锁」原则——`release_active` 在归档之前，与 cancel 路径和设计文档 §3.2 不一致。
- **P2（中）**：LLM 调用（`scheduler.schedule`）没有竞速取消——scheduler 卡死或长间隔流式时执行端不会及时停。
- **T3（中）**：P2 修复后的配套测试——hang 场景断言 runner 自行返回 `Superseded` 而非依赖 `abort()`。
- **T1（高）**：conformance 补幂等与 input-only 契约断言——否则接入方 SQL 实现可以不幂等也通过 L0 验收。
- **T2（中）**：harness 的 Cancel/Reap 步骤补归档——L1 场景层模拟与生产行为保持一致。

## 视觉/功能效果

无 UI 变化；纯后端正确性与测试完备性修正。

## 技术栈

Rust workspace（tokio async/await，trait 端口 + 分层架构），涉及 `nova-responses`、`nova-agent-runtime`、`verify/mock/server`、`verify/mock/agentd`、`verify/harness`、`verify/conformance`、`gateway`。

## 实现方案（已核实代码位置）

### P1：sweep 归档顺序修正

- 文件：`crates/responses/src/service/sweep.rs`
- 现状：tick 循环内顺序为 Failed 事件 + close（105-114）→ `release_active`（119-142）→ 归档块（148-185）。
- 修正：把归档块（`if claim.store { ... }` 整段）移动到 `release_active` 的 if-let 块之前，使顺序变为：Failed 事件 + close → **归档** → `release_active`。与 cancel 路径（`responses.rs:418-463` 先归档后释放）及 07 文档 §3.2「先 append_turn，再 advance，最后 release_active」对齐。

### P2：schedule 竞速取消

- 文件：`verify/mock/agentd/src/mock_runner.rs:91`
- 现状：`let outcome = match self.scheduler.schedule(&request, sink).await { ... }` 直接 await。
- 修正：

```rust
let outcome = tokio::select! {
    out = self.scheduler.schedule(&request, sink) => out,
    _ = cancel.cancelled() => return Err(AgentError::Superseded),
};
```

保留后续 match（Ok/Superseded/Failed）不变。select 丢弃 schedule future 后立即 return，sink 不再使用，无副作用。

### T3：hang 测试改造

- 文件：`gateway/tests/http_contract.rs` 的 `a_cancelled_turn_archives_completed_output`（874-923）
- 修正：cancel 后（`produced.notified().await` 与 HTTP cancel 之后）将 `handle.abort()` 替换为：

```rust
let result = handle.await.expect("join");
assert_eq!(result, Executed::Superseded,
    "a cancelled generation must stop on its own, not hang forever");
```

归档断言（input + "partial answer"）保持不变。默认 `cancel_poll_interval` 250ms 轮询会在真实时间内感知，测试可接受 <500ms 等待。

### T1：conformance 契约断言

- 文件：`verify/conformance/src/lib.rs` 的 `assert_context_conformance`（422-550）
- 插入位置：3-turn 循环的 read_snapshot 断言（turns==3、item_count==6，459-465 行）之后。
- (a) 幂等断言：用 `ids[2]` 重复 `append_turn`（任意 TurnCommit），断言返回 index==2；再 read_snapshot 断言 turns 仍为 3、item_count 仍为 6。
- (b) input-only 断言：新 `response_id` append_turn（`output_items: vec![]`，status=Failed），断言 turns==4、item_count==7、快照含该 input 文本。
- 连带调整：后续 delete 断言（495-507 行，现期望 turns==3/item_count==6）改为 4/7。

### T2：harness Cancel/Reap 补归档

- 文件：`verify/harness/src/scenario.rs`
- 新增私有 helper `archive_incomplete_turn`（harness 内实现，不能引用 sweep 的 pub(crate) `replay_completed_items`）：用 `ctx.world.event_log.read_after` 分页收集 `OutputItemDone` 的 `EventBody::Item`（按 output_index 排序），然后 `ctx.world.conversation.append_turn(input_items + items, status)`。
- `Step::Cancel`（662-679）ok 分支：在 `settle_session` 之前，若 record `is_stored()` 且 conversation-anchored，调用 helper（用 `record.spec.input_items`，回放 event_log）。
- `Step::Reap`（682-711）：对每个 claim，在 `release_active` 之前，若 `claim.store` 且有 `conversation_id`，调用 helper（用 `claim.input_items`）。
- 不改 `settle_session` 与 `Step::Complete`（Complete 已自行归档，避免重复路径；`append_turn` 幂等兜底）。
- 现有场景兼容性已核实：`reap-closes-lost-claim.yaml` 无 conversation 锚点（归档 no-op）、`cancel-cross-tenant-denied.yaml` 走 not_found 分支，均不受影响。

## 目录结构

```
nova-chat/
├── crates/responses/src/service/sweep.rs        # [MODIFY] P1：归档块移到 release_active 之前
├── verify/mock/agentd/src/mock_runner.rs        # [MODIFY] P2：schedule 用 select 竞速 cancel
├── gateway/tests/http_contract.rs               # [MODIFY] T3：hang 测试断言自行 Superseded
├── verify/conformance/src/lib.rs                # [MODIFY] T1：幂等 + input-only 契约断言
└── verify/harness/src/scenario.rs               # [MODIFY] T2：Cancel/Reap 步骤补归档
```

## 实现注意事项

- **P2 的 select 公平性**：`cancel.cancelled()` 是永不返回的 future（除非取消），select 默认随机分支不影响正确性；schedule 正常完成时立即返回。
- **T1 断言顺序**：幂等断言必须在 turns==3 基线断言之后、input-only 之前，否则计数基线混乱；后续 delete 断言期望值同步 +1。
- **T2 顺序**：归档必须在 `release_active` 之前（与生产一致），harness 的 `?` 传播错误（scenario 失败即 bail）。
- **回归风险**：P1 只移动代码块不改逻辑；P2 只影响 mock runner（生产 runner 由接入方实现 `AgentRunner`，契约文档已说明 cancel 参数语义）；T1/T2 纯增量断言。
- **验证**：`cargo check --workspace --all-targets` + `cargo test --workspace` 全量 + `read_lints` 改动文件。