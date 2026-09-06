# 设计文档

> 依据：[`../requirements/`](../requirements/) · [`../architecture/`](../architecture/)

---

## 规则

| 规则 | 说明 |
|------|------|
| 编号按完成顺序 | `design/` 下编号连续 |
| 仅编号文档可实现 | 对齐需求与生效 ADR |
| `drafts/` 不可直接实现 | 改写后取下一序号移出 |

---

## 正式设计

| # | 文档 | 产出 |
|---|------|------|
| **00** | [`00-architecture-review.md`](./00-architecture-review.md) | 逐组件核对的架构复验图（配图可伪证） |
| **01** | [`01-responses-api.md`](./01-responses-api.md) | Responses API：端点、三种响应模式、事件契约、状态码 |
| **02** | [`02-verification.md`](./02-verification.md) | Trace + Oracle；验证分层（L0–L3） |
| **03** | [`03-context-chain.md`](./03-context-chain.md) | 上下文链：物化快照（D24）、断裂、删除 |
| **04** | [`04-content-integrity.md`](./04-content-integrity.md) | 内容完整性：HMAC 防篡改 |
| **05** | [`05-reliability.md`](./05-reliability.md) | 可靠性：存储分层、故障语义、优雅停机、reap |
| **06** | [`06-protocol-subset.md`](./06-protocol-subset.md) | 对外可发布的协议子集规范 |
| **07** | [`07-conversations-and-sessions.md`](./07-conversations-and-sessions.md) | 会话容器（链尾指针，D27）与会话层（状态广播，D26）；关键时序图 |
| — | [`01-session-stream.md`](./01-session-stream.md) | **历史**：Session/Turn/快照/热冷层/镜像，D20 已移除，不描述当前系统 |

## 草稿

| 文档 | 状态 |
|------|------|
| [`drafts/stream-channel-adapters.md`](./drafts/stream-channel-adapters.md) | 选型对比（已落地：`adapters-event-log-redis` 生产缓冲；`adapters-mem` + `mem-server` 验证载体） |
| [`drafts/security.md`](./drafts/security.md) | 鉴权素材（后续） |

已并入 01 的 conversation / observation 草稿见 [`../archive/`](../archive/)。

---

## 路线图

```mermaid
%%{init: {"flowchart": {"curve": "basis", "rankSpacing": 55}}}%%
flowchart TB
    S0["✅ Responses 封闭子集（D20/21/22）"] --> S1
    S1["✅ D25：执行独立 + 缓冲共享化 + 能力层抽离"] --> S2
    S2["✅ 验证拓扑同构（mem-server）· baseline 100%"] --> S3
    S3["待接入：真实 provider 适配器"] --> S4
    S4["L3 接 Redis：真库端到端"]
```
