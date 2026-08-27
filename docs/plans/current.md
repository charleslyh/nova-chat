# 当期计划：验证先行 + Session 流闭环（D19）

> 状态：**展开级**
> 设计：[`../design/02-verification.md`](../design/02-verification.md) · [`../design/01-session-stream.md`](../design/01-session-stream.md)

---

## 1. 已锁定

| 项 | 出处 |
|----|------|
| 验证先行：Trace + Oracle + 分层 L0–L2 | D15 · 02 |
| 验收三层：API / mock 状态 / 链路 Trace | 02 |
| 产品 = Session 可回放消息服务 | D19 |
| 服务 = `nova-sessions-gateway`；POST+SSE 同进程 | D19 |
| meta ≠ stream | D11 |
| L0–L2 mem；无 Docker 前置 | D17 |

---

## 2. 交付与验收

| # | 交付 | 验收 |
|---|------|------|
| **V1** | `testing/harness`（Trace JSONL + Oracle） | ✅ `just verify l1` |
| **V2** | 能力场景：fence / gap / 幂等 / busy / double-claim / snapshot / sequential | ✅ L1 |
| **V3** | L2 HTTP + Trace：health / edge turn+agent / home idempotent | ✅ |
| **V4** | 覆盖补齐：FR/INV 诚实标标 + mid-snapshot + busy 二次拒绝；本期外项 deferred | ✅ `just coverage` in-scope OK |
| **V5** | FR-17：跨接入点游标续订（edge→home SSE） | ✅ `just verify l2` |
| **V6** | INV-32：只读降级拒写，观测仍可读 | ✅ L1 + L2 |
| **V7** | CR-8/INV-30 过载拒绝不损坏；停 edge 后游标续订 | ✅ L1 + L2 |
| **V8** | FR-18 全局 pending/在途阈值 → `Overloaded` / HTTP 429 | ✅ meta + L1/L2 |
| 1–6 | 既有产品闭环（gateway / mem / agent / sim） | `just verify` |

退出（近期）：L0/L1 绿且 Trace+Oracle 成为默认；能力场景持续补齐。`sim` 不替代 CI。

下一步候选：INV-33 客户端退避（偏客户端）；完整压力平台。

---

## 3. 不做什么

用 sim 当正确性门禁；生产默认开 Trace；本期完整压力/灾备平台（先场景化再扩展）。

---

## 4. 变更日志

| 日期 | 变更 |
|------|------|
| 2026-08-27 | V8：pending_limit → Overloaded/429（FR-18 全局在途阈值） |
| 2026-08-27 | V7：overload-reject-consistent + stop-edge-resume |
| 2026-08-27 | V6：INV-32 read-only 拒写（admin + L1/L2） |
| 2026-08-27 | V5：cross-instance-resume（FR-17 edge→home 游标续订） |
| 2026-08-27 | V4：covers 补齐 + mid-snapshot / busy 二次拒绝；coverage in-scope OK |
| 2026-08-27 | L1 能力场景 fence/gap/idempotent；L2 Trace HTTP 场景 |
| 2026-08-27 | 验证先行：02-verification + harness Trace/Oracle |
| 2026-08-27 | 包结构：`gateway` / `core` / `adapters/*`；`testing/` · 根目录 `xtask/` |
| 2026-08-27 | D19 升为当期主计划 |
