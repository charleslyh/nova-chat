# 落地推进

> 依据：[`../requirements/`](../requirements/) · D20 / D21 / D22 · [`../design/01-responses-api.md`](../design/01-responses-api.md)

---

## 三级详略

| 级别 | 适用 |
|---|---|
| 展开级 | 唯一当期 → [`current.md`](./current.md) |
| 入口级 | 仅下一期 |
| 标题级 | 更远 |

---

## 迭代总览

| 轮次 | 目标 | 级别 |
|---|---|---|
| 已完成 | 验证先行 + Session 流最小闭环（V1–V9） | — |
| 已完成 | mem 驱动主框架（热冷恢复 · 开屏 · Mirror）（V10–V13） | — · **成果被 D20 推翻** |
| **已完成** | **Responses 协议子集重构**（W 期）：主资源收敛为单次生成 · 存储与订阅分离 · 封闭子集 · 真实 SQL 承载 | 展开 → [`current.md`](./current.md) |
| **已完成** | **D25：执行独立 + 在途缓冲共享化 + 能力层抽离**（X/Y 期） | 展开 → [`current.md`](./current.md) |
| **已完成** | **验证拓扑同构**（Z 期）：mem 共享载体（mem-server + mem-client + 独立 sweep），L2 与生产同构，baseline 覆盖 100% | 展开 → [`current.md`](./current.md) |
| 下一期 | **真实 provider 适配器接入**；L3 接 Redis（真库端到端） | 入口 |
| 更远 | 生产压测平台；多密钥并存；细粒度鉴权票 | 标题 |

> 下一期：provider 接入是产品需求驱动（见 `decisions.md` D25 的「具体实现后置」）；L3 接 Redis 则把真库端到端跑通。

---

## 命令

| 轨 | 命令 | Docker |
|---|---|---|
| 验证 | `just verify`（或 `verify l0\|l1\|l2\|l3`）· `just coverage` · `just check-deps` | L0–L2 否；L3 可选 |
| 人工 | `just sim` → `/chat` 演示多轮链 | 否 |
| 部署 | `just deploy` | 可选 |

---

## 变更日志

| 日期 | 变更 |
|---|---|
| 2026-09-03 | **Z 期完成**：验证拓扑同构。mem 改为共享载体（`nova-responses-mem-server` + `adapters-mem-client`），执行/维护独立进程（`nova-agentd`/`nova-responses-sweep`），L2 与生产同构，baseline 覆盖 100% |
| 2026-09-01 | **X/Y 期完成**：D25 执行独立 + 在途缓冲共享化 + 能力层独立成 crate。新增 `nova-responses` / `nova-agentd` / `adapters-event-log-redis`；删除转发子系统 |
| 2026-09-01 | **W 期完成**：Responses 协议子集重构。新增 D20/D21/D22；spec 升 v3；新增 `adapters-sql` 与 L3；服务改名 `nova-responses-*` |
| 2026-09-01 | V10–V13 成果（热冷分层 / 开屏快照 / 跨区 Mirror）由 D20 整体废止 |
| 2026-08-27 | 当期改为「mem 完备主框架 / 延后 ports」 |
| 2026-08-27 | 文档重构：当期计划更名 `current.md`；任务系统文档归档 |
| 2026-08-27 | D19 范围收口 |
