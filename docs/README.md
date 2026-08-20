# nova-nats 文档

分布式任务系统：多区域提交 · 容量感知调度 · 分布式执行 · 跨区域流式观测与协作。

---

## 文档地图

```mermaid
%%{init: {"flowchart": {"curve": "basis", "rankSpacing": 75, "nodeSpacing": 28}}}%%
flowchart LR
    R["<b>requirements/</b><br/>要什么"] --> A["<b>architecture/</b><br/>怎么定的 · 不许违反什么"]
    A --> D["<b>design/</b><br/>怎么做"]
    AR["<b>archive/</b><br/>历史追溯"] -.已失效.-> R
```

| 目录 | 角色 | 变更频率 |
|------|------|---------|
| [`requirements/`](./requirements/) | **要什么**——需求与参数，唯一依据 | 低（需求）/ 中（参数） |
| [`architecture/`](./architecture/) | **怎么定的**——决策记录与设计不变量 | 低 |
| [`design/`](./design/) | **怎么做**——按步骤展开的设计 | 高 |
| [`archive/`](./archive/) | 历史文档，**不作为设计依据** | 冻结 |

---

## 文档清单

### requirements/ — 需求基线

| 文档 | 内容 |
|------|------|
| [`spec.md`](./requirements/spec.md) | 需求规格：21 条 FR、11 条 CR、17 条质量需求、16 项冲突矩阵、范围界定 |
| [`parameters.md`](./requirements/parameters.md) | 量化参数：业务锚点、守恒关系、导出量、SLO、适用区间与越界阈值 |

> **拆分原因**：需求相对稳定，参数需随实测校准。二者变更节奏不同。

### architecture/ — 决策与约束

| 文档 | 内容 |
|------|------|
| [`decisions.md`](./architecture/decisions.md) | 11 项架构决策（D1~D11）及其理由、代价、备选方案 |
| [`invariants.md`](./architecture/invariants.md) | 37 条设计不变量（INV-1~INV-37），违反任一即某条 CR 不成立 |

### design/ — 分步设计

路线图与规范见 [`design/README.md`](./design/README.md)。

| 文档 | 性质 | 说明 |
|------|------|------|
| [`01-claim-and-match.md`](./design/01-claim-and-match.md) | ✅ **实现依据** | 领取与匹配 |
| [`drafts/observation.md`](./design/drafts/observation.md) | ⛔ **草稿，禁止实现** | 观测层素材，含与 D8 冲突的多区域方案 |
| [`drafts/security.md`](./design/drafts/security.md) | ⚠️ **草稿，禁止实现** | 鉴权素材，缺 DSL 沙箱防护 |

> **编号按完成顺序分配**，因此 `design/` 下的编号永远连续。未开始的步骤不预留文件与编号。
> `drafts/` 内为未对齐基线的素材，改写完成后取下一序号移出。

---

## 阅读路径

| 目的 | 顺序 |
|------|------|
| **首次了解系统** | `requirements/spec.md` §1~§4 → `architecture/decisions.md` 索引 → `design/README.md` |
| **参与设计评审** | `architecture/invariants.md`（必读）→ 对应 `design/` 文档 |
| **评估容量与选型** | `requirements/parameters.md` §4 §7 |
| **质疑某项决策** | `architecture/decisions.md` 对应条目（含备选方案与否决理由） |
| **实现某个模块** | 对应 `design/` 文档 + `architecture/invariants.md` 相关小节 |

---

## 核心约束速览

新加入者最容易踩的七条：

| # | 约束 | 出处 |
|---|------|------|
| 1 | **领取必须是单点条件更新**，不可先查后改 | INV-1 |
| 2 | **提交幂等不得依赖 TTL 窗口**，须「存在即拒绝」 | INV-2 |
| 3 | **容量核算不得采信设备自报** | INV-3 / D10 |
| 4 | **尝试序号严格单调，永不重置**（四处语义共用） | INV-5 |
| 5 | **一切重建起点必须记录序号** | INV-13 |
| 6 | **每个非终态都必须有超时出口**，否则任务会永久停滞 | INV-34 |
| 7 | **领取路径与输出路径不共用承载**（吞吐差 4 个数量级） | D11 |

---

## 文档纪律

| 规则 | 说明 |
|------|------|
| 需求不引用设计 | `requirements/` 自闭环，只描述「要什么」 |
| 设计引用需求编号 | 每项设计标注其满足的 FR/CR 编号 |
| 不变量集中管理 | 跨步骤的「不可违反」条目统一进 `invariants.md` |
| 决策不删改 | 变更时新增 `SUPERSEDED BY` 记录，保留追溯链 |
| 归档不被引用 | 现行文档不得引用 `archive/`；如需其结论，先提取到现行文档 |
| 图样统一 | 流程图使用 `%%{init: {"flowchart": {"curve": "basis"}}}%%` 曲线连线 |
