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
| **当期** | 验证先行（Trace+Oracle）+ gateway 闭环 | 展开 → [`current.md`](./current.md) |
| 下一期 | 能力场景包；L2 共享 Trace；热 miss/冷层 | 入口 |
| 更远 | 压力/HA/灾备场景化；Redis；Mirror；鉴权 | 标题 |

---

## 命令

| 轨 | 命令 | Docker |
|----|------|--------|
| 验证 | `just verify`（或 `verify l0\|l1\|l2`）· `just sim` | 否 |
| 部署 | `just deploy` | 可选 |

---

## 变更日志

| 日期 | 变更 |
|------|------|
| 2026-08-27 | 文档重构：当期计划更名为 `current.md`；任务系统文档归档 |
| 2026-08-27 | D19 范围收口 |
