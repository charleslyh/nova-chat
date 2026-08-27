# 落地推进

> 依据：[`../requirements/`](../requirements/) · D19 · [`../design/01-session-stream.md`](../design/01-session-stream.md)

---

## 三级详略

| 级别 | 适用 |
|------|------|
| 展开级 | 唯一当期 → [`current.md`](./current.md) |
| 入口级 | 仅下一期 |
| 标题级 | 更远 |

---

## 迭代总览

| 轮次 | 目标 | 级别 |
|------|------|------|
| **已完成** | 验证先行 + Session 流最小闭环（V1–V9） | — |
| **当期** | **mem 驱动主框架完备**（热/冷恢复 · 开屏 · Mirror 语义 · 无粘性）；**延后真实 ports** | 展开 → [`current.md`](./current.md) |
| 下一期 | 真实 stream/meta/snapshot 适配器选型与 L0 契约过线 | 入口 |
| 更远 | 生产压测平台；跨机 Mirror；鉴权 | 标题 |

---

## 命令

| 轨 | 命令 | Docker |
|----|------|--------|
| 验证 | `just verify`（或 `verify l0\|l1\|l2`）· `just coverage` · `just sim` | 否 |
| 部署 | `just deploy` | 可选 |

---

## 变更日志

| 日期 | 变更 |
|------|------|
| 2026-08-27 | 当期改为「mem 完备主框架 / 延后 ports」；下一期才接真实适配器 |
| 2026-08-27 | 文档重构：当期计划更名为 `current.md`；任务系统文档归档 |
| 2026-08-27 | D19 范围收口 |
