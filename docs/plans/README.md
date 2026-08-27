# 落地推进计划

> 执行视图：与 [`../design/README.md`](../design/README.md) §3 路线图对齐，**不复制**其设计顺序正文。
> 依据：[`../requirements/`](../requirements/) · [`../architecture/decisions.md`](../architecture/decisions.md)（含 D14–D18）

---

## 1. 三级详略与单向展开

| 级别 | 适用 | 允许 | 禁止 |
|------|------|------|------|
| **展开级** | 唯一当期迭代 | 完整设计要点、端口签名引用、验收命令、遗留问题 | — |
| **入口级** | 仅下一期 | 目标、覆盖需求编号、入口条件、待定夺议题 | 表结构/接口签名/场景细节 |
| **标题级** | 更远迭代 | 一行标题 + 覆盖需求编号 | 其他一切 |

**规则**：验收引入需求变更 → 先回写 `requirements/` 与 `decisions.md`（`SUPERSEDED BY`）→ 再展开下一期。禁止提前展开非当期内容。

---

## 2. 迭代总览

| 轮次 | 目标 | 覆盖需求（摘要） | 涉及模块 | 当轮技术栈定夺 | 退出条件 | 文档级别 |
|------|------|------------------|----------|----------------|----------|----------|
| **迭代 0** | 基线补全 + 端口骨架 + **验证框架** | FR-2 子项、CR-1/3/10、相关 INV 的**验证能力** | core/ports/matcher/claim/adapter-mem/conformance/testkit/server | Rust、端口划分、mem 沙箱、虚拟时钟、本机多进程拓扑 | `just verify l0/l1/l2`；`just procs up`；`just coverage` | **展开级** → [`iteration-0.md`](./iteration-0.md) |
| **迭代 1** | 任务池与存储 → `design/02` | FR-4/21/22/23/24、CR-2/3/7/11、INV-1/2/4/29/30/34 | 新存储适配器、claim、patrol | 存储产品、表结构、幂等闸门形态、背压阈值 | 新适配器过 conformance；相关场景绿 | **入口级**（见下） |
| **迭代 2** | 生命周期与事件模型 → `design/03` | FR-3/5/6/20、CR-7/8 | — | — | — | 标题级 |
| **迭代 3** | 观测与协作（改写 observation + conversation） | FR-9~15、CR-4/5/6、SR-2/5、**D18** | — | StreamChannel `StreamId`、第一输出适配器、Session 快照协议 | — | 标题级 |
| **迭代 4** | 身份与幂等 | FR-15/22、CR-2、INV-25~28 | — | — | — | 标题级 |
| **迭代 5** | 鉴权与安全（补 DSL 沙箱） | CR-9、SEC-1~6 | — | — | — | 标题级 |
| **迭代 6** | 可观测性与容量治理 | OR-1~6 | — | — | — | 标题级 |

迭代 0 **不**产出 `design/` 编号文档。迭代 1–6 对应 design 步骤 2–7。

对话层 / StreamChannel 工作包（不插队实现）：见 [`conversation-and-stream.md`](./conversation-and-stream.md)（D18）。

### 迭代 1 入口级（下一期）

| 项 | 内容 |
|----|------|
| 目标 | 任务池与存储正式设计与真实存储适配器 |
| 入口条件 | 迭代 0 验收通过；D14 端口稳定；mem conformance 绿 |
| 待定夺 | 存储产品选型、表/索引、幂等落地、背压阈值、同库查询与领取隔离（FR-24） |
| 覆盖 | 见上表 |

### 草稿改写归属

| 草稿 | 归属轮次 |
|------|----------|
| `design/drafts/observation.md` | 迭代 3（与 conversation 合并改写） |
| `design/drafts/conversation.md` | 迭代 3（D18） |
| `design/drafts/stream-channel-adapters.md` | 迭代 3（StreamChannel 第一适配器锁定时参照） |
| `design/drafts/security.md` | 迭代 5 |

---

## 3. 双轨自动化

| 轨 | 命令 | Docker？ |
|----|------|----------|
| **验证** | `just verify l0\|l1\|l2` · `just procs up\|down` · `just coverage` · `just check-deps` | **否** |
| **部署** | `just deploy up\|down` | **是**（本机无 Docker 可跳过，不挡 L0–L2） |

场景源：`scenarios/l1/`、`scenarios/l2/`。本机拓扑配置：`config/`。部署：`deploy/docker/`。

---

## 4. 变更日志

| 日期 | 变更 |
|------|------|
| 2026-08-26 | 新增 [`stream-channel-adapters.md`](../design/drafts/stream-channel-adapters.md)：Redis Streams vs JetStream |
| 2026-08-25 | D18/计划补强：Redis vs JS 性能成本；Session 用量先验（conversation §0.1） |
| 2026-08-25 | D18 补丁：跨区回源分期、热→冷、Redis Streams 默认；见 [`conversation-and-stream.md`](./conversation-and-stream.md) |
| 2026-08-25 | 锁定 **D18**（Session 日志 / Turn=Task）；迭代 3 并入 `conversation` 草稿 |
| 2026-08-24 | 建立 plans/；迭代 0 展开；锁定 D14–D17 与双轨验证 |
