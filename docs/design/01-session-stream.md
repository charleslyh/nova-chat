> [!WARNING]
> **本文描述的系统已不存在。** Session / Turn / 快照开屏 / 热层冷层 / 跨区镜像等概念在 D20
> 收口时全部移除，对应的 `/v1/sessions/*` 端点与 `/v1/admin/trim_hot` 已从代码中删除。
>
> 当前架构见 [`00-architecture-review.md`](./00-architecture-review.md)；
> 现行协议面见 [`01-responses-api.md`](./01-responses-api.md)。
>
> 保留本文是因为它记录了被取代的方案，有助于理解 D20 为何这样收口——**但不要照它实现任何东西**。

> **SUPERSEDED BY [`01-responses-api.md`](./01-responses-api.md)（2026-09-01）**
>
> 本文描述 Session 级流式契约（建会话、发 Turn、快照开屏、热层冷层、跨区镜像），
> 该产品形态已由 D20 / D22 整体收口为「单次生成 + Responses 协议封闭子集」。
>
> 保留此存根仅为让既有链接不致断裂。内容不再维护，请勿据此实现。
