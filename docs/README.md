# 文档地图

> 服务：**nova-responses** — 对齐 OpenAI Responses 协议封闭子集的生成服务

---

## 先读这三条边界

| # | 命题 | 出处 |
|---|---|---|
| 1 | **存储与订阅分离**：持有生成条目用于拼接上下文；不提供会话线程级订阅与开屏还原 | [D20](./architecture/decisions.md#d20-交付边界收口存储与订阅分离) |
| 2 | **协议是封闭子集**：子集外一律 400，不静默忽略；严进同时更安全更兼容 | [D22](./architecture/decisions.md#d22-协议封闭子集与严格拒绝) |
| 3 | **可靠性分层**：内容库与账本上真库高可用；在途缓冲留进程内存 + 四项缓解 | [D21](./architecture/decisions.md#d21-可靠性分层三类存储的差异化投入) |

---

## 阅读顺序

```
requirements/spec.md          需求与编号基线（FR / CR / SEC）
        ↓
requirements/parameters.md    量化锚点与容量模型
        ↓
architecture/decisions.md     D20 / D21 / D22 → D11(RESTATED) → D14/D15/D17
        ↓
architecture/invariants.md    不变量（违反即某条 CR 不成立）
        ↓
design/01-responses-api.md    对外契约与实现依据
```

---

## 目录

### 需求

| 文档 | 内容 |
|---|---|
| [`requirements/spec.md`](./requirements/spec.md) | v3.0 · FR-1~39 / CR-1~13 / SR / OR / SEC-1~10 · 范围界定 |
| [`requirements/parameters.md`](./requirements/parameters.md) | v3.0 · 业务锚点 → 导出量 → SLO · **崩溃损失率量化** |

### 架构

| 文档 | 内容 |
|---|---|
| [`architecture/decisions.md`](./architecture/decisions.md) | ADR。**不删改正文**，变更用 `SUPERSEDED BY` 追溯 |
| [`architecture/invariants.md`](./architecture/invariants.md) | v3.0 · 不变量与「不可抽象清单」 |
| [`architecture/README.md`](./architecture/README.md) | 组件速览 |
| [`architecture/arc.md`](./architecture/arc.md) | 早期推导，已由 D20–D22 收口 |

### 设计

| 文档 | 内容 |
|---|---|
| [`design/01-responses-api.md`](./design/01-responses-api.md) | 端点、事件、`starting_after`、两条转发路径、状态码 |
| [`design/02-verification.md`](./design/02-verification.md) | L0–L3、裁判清单、场景矩阵 |
| [`design/03-context-chain.md`](./design/03-context-chain.md) | 数据模型、走链（内存单锁 vs SQL 递归）、上限、链亲和退役 |
| [`design/04-content-integrity.md`](./design/04-content-integrity.md) | 规范化、HMAC、密钥生命周期 |
| [`design/05-reliability.md`](./design/05-reliability.md) | 故障语义分层、四项缓解、升级触发条件 |
| [`design/06-protocol-subset.md`](./design/06-protocol-subset.md) | **对外可发布契约** |
| [`design/drafts/security.md`](./design/drafts/security.md) | 安全检查清单 |

### 计划

[`plans/current.md`](./plans/current.md) · [`plans/README.md`](./plans/README.md)

---

## 写作纪律

| 规则 | 理由 |
|---|---|
| 需求不引用设计 | 需求描述可观察行为，引用设计会锁死实现 |
| 设计引用 FR/CR/INV 编号 | 使每条设计可追溯到需求 |
| ADR 正文不删改，用 `SUPERSEDED BY` | 保留「为何曾这样决定」，否则重复踩坑 |
| **归档内容不被现行文档引用** | 归档前须先把结论提取进正文 |
| mermaid 统一 `%%{init: {"flowchart": {"curve": "basis"}}}%%` | 渲染一致 |

---

## 术语

刻意不使用「A 类 / B 类数据」这类分类：本服务在任何代码路径上都不做此判断。统一使用：

| 术语 | 含义 |
|---|---|
| **生成**（Response） | 一次模型调用的生命周期与结果对象；对外唯一主资源 |
| **条目**（Item） | 生成的输入或输出单元；协议封闭子集的成员 |
| **事件流** | 生成期间的增量事件；序号 0 基连续 |
| **在途事件缓冲** | 承载事件流的进程内有界环；不持久化 |
| **上下文库** | 持久化生成条目，支撑走链拼接 |
| **上下文链** | 由 `previous_response_id` 连成的生成序列 |
| **宿主节点** | 持有某次生成在途缓冲的网关进程 |
| **执行端** | 领取生成、写事件、终态提交输出条目的组件 |

> 完整渲染事件历史（transcript）**不是本服务的概念**：不定义、不存储、不判断。由调用方从实时流自行构建并持有。
