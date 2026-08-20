> ⚠️ **本文档已归档（2026-08），不作为设计依据。**
> 现行文档见 [`../README.md`](../README.md) · [`../requirements/spec.md`](../requirements/spec.md) · [`../architecture/invariants.md`](../architecture/invariants.md)。
> 保留目的：追溯早期架构探讨的决策由来。**文内链接可能已失效，属预期情况。**其中仍然有效的结论已提取至 `requirements/` 与 `architecture/`。

---

# nova-nats 全景架构基线

> 版本：v0.1（讨论基线）
> 目标场景：多区域高并发任务提交 + 全球分散的算力 Worker（受**可插拔放置策略**驱动的跨区调度）+ 任务需按 capacity 匹配领取（非严格 FIFO）
> 配套文档：
> - [`core-design.md`](./core-design.md) —— ⭐ **最小可行子集**（1 个有状态系统 + 2 个二进制即满足 R1–R6）。**建议先读这份**
> - [`realtime-stream-collab.md`](./realtime-stream-collab.md) —— 实时流式输出、跨区域多用户共享观测与互动（回答本文 §11.8）
> - [`task-identity-and-sot.md`](./task-identity-and-sot.md) —— 任务身份、状态权威、孤儿任务与双写规避（回答本文 §11.10）
> - [`security-authz.md`](./security-authz.md) —— 鉴权授权，含流式长连接的权限撤销与跨区域授权
>
> ⚠️ **本文是「全量目标态」，不是 Day-1 落地方案。** 其中 Redis Task Pool / Scheduler / Lease Reaper / etcd Control Plane 四个组件由公理 A6（按数值 capacity 匹配、可跳过）驱动；若不需要该语义，可全部删除，见 `core-design.md` §2。

---

## 0. 设计公理（先立骨架，后填细节）

这几条是后续所有决策的依据，讨论时如果要推翻某个设计，请先检查是否与这些公理冲突。

| # | 公理 | 推论 |
|---|------|------|
| A1 | **任务池是数据结构，不是消息流** | 任务状态存在可原子操作的状态存储（Redis/DB），不存在消息队列里<br/>⚠️ **该公理的唯一论据是 A6**。若不需要 A6，应改用消息流（见 `core-design.md` §2） |
| A2 | **领取是 CAS，必须落在单一强一致点** | 每个任务有唯一 owner Cell；跨 Cell 不竞争同一任务 |
| A3 | **跨区域强一致不可得（CAP）** | 用「静态分片 + 单 Cell 强一致」规避，而非追求全网一致 |
| A4 | **消息队列负责事件与通知，不负责状态** | NATS Core 通知、JetStream 事件流、Kafka 数仓管道 |
| A5 | **一切执行必须幂等** | 任务带全局唯一 ID + 幂等键，重复领取由执行侧兜底 |
| A6 | **相对有序，允许跳过** | 待领取池按优先级排序，但领取时可跳过 capacity 不匹配的任务 |
| A7 | **Cell 是故障与扩展的最小单元** | 单 Cell 挂掉只影响其分片，不影响全局 |
| A8 | **放置策略是可插拔输入，不是架构假设** | 任务域不感知任何具体信号口径；策略只输出「区域权重表」；策略层可最终一致，状态层必须强一致 |

---

## 1. 节点清单（含你未列出的补充项）

### 1.1 接入层
| 节点 | 是否分布式 | 有状态 | 职责 |
|------|-----------|--------|------|
| `Browser / CLI / SDK` | N/A | 客户端本地态 | 发起任务、订阅进度 |
| `GSLB / DNS` | 全球 | 无 | 智能解析，就近接入 |
| `CDN` | 全球边缘 | 缓存态 | 静态资源、大文件分发 |
| `API Gateway` | 多区域多副本 | 无 | TLS 卸载、鉴权、限流、路由 |

### 1.2 Web / BFF 层
| 节点 | 是否分布式 | 有状态 | 职责 |
|------|-----------|--------|------|
| `Web/BFF Node` | 多区域多副本 | **无状态** | SSR、聚合 API、参数校验 |
| `Realtime Gateway`（SSE/WS） | 多区域多副本 | **有连接态** | 长连接持有、进度推送；需连接路由 |
| `Auth Service` | 多副本 | 无（JWT 自验证） | 签发/校验 token、账户与配额 |

### 1.3 任务域（核心）⭐
| 节点 | 是否分布式 | 有状态 | 职责 |
|------|-----------|--------|------|
| `Task Ingress API` | 多区域多副本 | **无状态** | 任务收单、幂等去重、配额校验 |
| `Global Task Router` | 多副本（读全局路由表） | 读缓存 | 决定任务归属哪个 Cell |
| `Task Scheduler / Matcher` | **每分片单 Leader** | **有状态（分片 owner）** | capacity 匹配、优先级仲裁、抢占 |
| `Task Pool`（Redis Cluster） | 每 Cell 一套 | **强状态** | 待领取池/延迟池/执行中/租约/去重 |
| `Task Metadata DB` | 分片 + 主从 | **强状态** | 任务定义、最终状态、审计、计费依据 |
| `Lease Reaper` | 每分片单 Leader | 无（读池） | 扫描超时租约，回收重派 |
| `Result Collector` | 多副本 | 无状态 | 结果校验、落库、触发下游 |

### 1.4 Worker 域
| 节点 | 是否分布式 | 有状态 | 职责 |
|------|-----------|--------|------|
| `Worker Agent` | **全球分散，必定分布式** | 本地执行态 | 注册、上报 capacity、拉取、执行、心跳、续租 |
| `Capacity Registry` | 每 Cell 一套 | **状态（TTL）** | Worker 存活、剩余 capacity 索引 |
| `Runtime Sandbox` | 随 Worker | 临时态 | 隔离执行（容器/VM/进程） |
| `Object Storage` | 全球 + 区域桶 | **强状态** | 任务输入输出大文件（S3/COS） |

### 1.5 业务状态域
| 节点 | 是否分布式 | 有状态 | 职责 |
|------|-----------|--------|------|
| `Business State Service` | 多副本 | **无状态计算** | 业务实体状态机（订单/作业/项目） |
| `Business DB` | 分片 + 主从 | **强状态** | 业务事实数据 |
| `Cache Layer` | 每区域 | 缓存态 | 热点读缓存 |

### 1.6 消息 / 事件层（多种，职责不同）⭐
| 节点 | 是否分布式 | 用途 |
|------|-----------|------|
| `NATS Core Cluster` | 每区域集群 + Gateway 互联 | 轻量通知、心跳、控制指令、Request/Reply RPC、推送 fanout |
| `NATS JetStream` | 每 Cell R3 + 跨区 Mirror | 任务生命周期事件流（审计/溯源）、可靠结果回传、跨区容灾复制 |
| `Kafka / TDMQ`（可选） | 集中或多区 | 指标/日志/计费流水 → 数仓、离线分析 |

### 1.7 控制平面（**你的列表里缺的关键一块**）
| 节点 | 是否分布式 | 有状态 | 职责 |
|------|-----------|--------|------|
| `Global Control Plane`（etcd/Raft） | 跨区 3/5 节点 | **强一致** | 分片映射表、Cell 注册、Leader 选举、全局配额 |
| `Config Center` | 多副本 | 强一致 | 配置下发、灰度、Feature Flag |
| `Service Discovery` | 多副本 | 状态 | 服务实例发现与健康 |

### 1.8 放置策略 / 弹性调度（**必须单列**）

本层的作用是把「任务应该去哪个 Cell 执行」从架构中**解耦为可替换策略**。架构本身**不依赖任何具体的信号口径**——放置策略是业务可插拔的输入，不是架构假设。

| 节点 | 职责 |
|------|------|
| `Placement Signal Collector` | 采集各区域的**放置信号**（signal），信号种类可插拔 |
| `Placement Planner` | 将信号聚合为**区域权重表**，影响任务路由与扩缩容决策 |
| `Autoscaler` | 按队列积压 + 区域权重弹性扩缩 Worker |
| `Region Health Monitor` | Cell 健康度评分，驱动故障转移 |

**放置信号是抽象接口，不是具体指标。** 常见维度（按业务自行选择与加权）：

| 信号类别 | 语义 | 变化频率 |
|---------|------|---------|
| `unit_cost` | 单位算力的相对代价（无量纲，归一化后比较） | 慢（分钟~小时） |
| `headroom` | 区域可用容量余量 | 快（秒级） |
| `backlog` | 待领取积压量与 P95 等待时长 | 快（秒级） |
| `proximity` | 到任务发起方的网络时延 | 慢（准静态） |
| `reliability` | 历史成功率、故障频次评分 | 慢 |
| `constraint` | 硬约束：数据驻留、合规、专属资源池 | 事件驱动 |

> `constraint` 与其余信号性质不同：**它是过滤器（filter），不参与加权**。先过滤出合法候选集，再在候选集内按权重择优。把硬约束混入加权会导致"权重足够高就能违规"的严重错误。

**设计要求**：`Placement Planner` 必须可在**不改动任务域任何代码**的前提下替换策略实现（读同一份信号接口、写同一张权重表）。这保证放置策略的演进不会波及强一致核心。

### 1.9 可观测性
`Metrics(Prometheus)` · `Logs(Loki/ES)` · `Traces(OTel/Jaeger)` · `Alerting` · `Dashboard`

---

## 2. 全景架构图

```mermaid
%%{init: {"flowchart": {"curve": "basis"}}}%%
flowchart TB
    subgraph CLIENT["① 客户端层"]
        BR["Browser / SPA"]
        CLI["CLI / SDK"]
        MOB["Mobile App"]
    end

    subgraph EDGE["② 全球接入层"]
        GSLB["GSLB / DNS<br/>就近解析"]
        CDN["CDN 边缘节点"]
        GW["API Gateway ×N<br/>鉴权·限流·路由"]
    end

    subgraph BFF["③ Web / BFF 层（无状态，多区域多副本）"]
        WEB["Web/BFF Node ×N"]
        RT["Realtime Gateway ×N<br/>SSE / WebSocket<br/>（有连接态）"]
        AUTH["Auth Service ×N"]
    end

    subgraph CP["④ 全局控制平面（强一致，跨区 Raft）"]
        ETCD["etcd / Raft<br/>分片映射·Leader 选举·全局配额"]
        CFG["Config Center"]
        PLAN["Placement Planner<br/>放置信号 → 区域权重表"]
        HEALTH["Region Health Monitor"]
    end

    subgraph INGRESS["⑤ 任务收单层（无状态）"]
        TAPI["Task Ingress API ×N<br/>幂等·配额·校验"]
        ROUTER["Global Task Router ×N<br/>hash(key) → Cell"]
    end

    subgraph CELLA["⑥-A Region Cell · Region-A"]
        direction TB
        SCHA["Scheduler/Matcher<br/>（分片 Leader）"]
        POOLA[("Task Pool<br/>Redis Cluster")]
        REAPA["Lease Reaper"]
        NCA["NATS Core Cluster"]
        JSA[("JetStream R3")]
        CAPA[("Capacity Registry")]
    end

    subgraph CELLB["⑥-B Region Cell · Region-B"]
        direction TB
        SCHB["Scheduler/Matcher<br/>（分片 Leader）"]
        POOLB[("Task Pool<br/>Redis Cluster")]
        REAPB["Lease Reaper"]
        NCB["NATS Core Cluster"]
        JSB[("JetStream R3")]
        CAPB[("Capacity Registry")]
    end

    subgraph CELLC["⑥-C Region Cell · Region-C"]
        direction TB
        SCHC["Scheduler/Matcher<br/>（分片 Leader）"]
        POOLC[("Task Pool<br/>Redis Cluster")]
        NCC["NATS Core Cluster"]
        JSC[("JetStream R3")]
        CAPC[("Capacity Registry")]
    end

    subgraph WORKER["⑦ Worker 层（全球分散，必定分布式）"]
        WA["Worker Agent<br/>Region-A ×N"]
        WB["Worker Agent<br/>Region-B ×N"]
        WC["Worker Agent<br/>Region-C ×N"]
    end

    subgraph STATE["⑧ 业务状态与持久层"]
        BIZ["Business State Service ×N"]
        MDB[("Task Metadata DB<br/>分片+主从")]
        BDB[("Business DB")]
        OSS[("Object Storage<br/>输入/输出大文件")]
        CACHE[("Cache 每区域")]
    end

    subgraph PIPE["⑨ 数据管道 / 数仓"]
        KFK["Kafka / TDMQ"]
        DW[("数仓 / OLAP")]
        BILL["计费 / 报表"]
    end

    subgraph OBS["⑩ 可观测性"]
        MET["Metrics"]
        LOG["Logs"]
        TRC["Traces"]
        ALT["Alerting"]
    end

    BR --> GSLB
    CLI --> GSLB
    MOB --> GSLB
    GSLB --> CDN
    GSLB --> GW
    BR -.静态资源.-> CDN
    GW --> WEB
    GW --> RT
    GW --> AUTH

    WEB --> TAPI
    WEB --> BIZ
    RT -.订阅进度.-> NCA
    RT -.订阅进度.-> NCB
    RT -.订阅进度.-> NCC

    TAPI --> ROUTER
    ROUTER -.读分片表.-> ETCD
    PLAN --> ETCD
    HEALTH --> ETCD
    CFG -.配置下发.-> SCHA
    CFG -.配置下发.-> SCHB

    ROUTER ==路由到归属 Cell==> POOLA
    ROUTER ==> POOLB
    ROUTER ==> POOLC

    SCHA <--> POOLA
    SCHA --> NCA
    SCHA --> JSA
    REAPA --> POOLA
    CAPA <--> POOLA

    SCHB <--> POOLB
    SCHB --> NCB
    SCHB --> JSB
    REAPB --> POOLB
    CAPB <--> POOLB

    SCHC <--> POOLC
    SCHC --> NCC
    SCHC --> JSC
    CAPC <--> POOLC

    NCA -.新任务通知.-> WA
    NCB -.新任务通知.-> WB
    NCC -.新任务通知.-> WC
    WA ==原子领取/续租==> POOLA
    WB ==> POOLB
    WC ==> POOLC
    WA -.心跳+capacity.-> CAPA
    WB -.心跳+capacity.-> CAPB
    WC -.心跳+capacity.-> CAPC

    WA <-.大文件.-> OSS
    WB <-.大文件.-> OSS
    WC <-.大文件.-> OSS

    JSA ==结果/事件==> BIZ
    JSB ==> BIZ
    JSC ==> BIZ
    BIZ --> BDB
    BIZ --> CACHE
    SCHA --> MDB
    SCHB --> MDB
    SCHC --> MDB

    JSA -.Mirror 灾备.-> JSB
    NCA <-.Gateway.-> NCB
    NCB <-.Gateway.-> NCC
    NCA <-.Gateway.-> NCC

    JSA --> KFK
    JSB --> KFK
    JSC --> KFK
    KFK --> DW
    DW --> BILL
    Autoscaler["Autoscaler"] --> WORKER
    PLAN --> Autoscaler

    CELLA -.-> OBS
    CELLB -.-> OBS
    CELLC -.-> OBS
    WORKER -.-> OBS
    BFF -.-> OBS
```

**图例约定**
- `==>` 粗实线 = **强一致关键路径**（原子领取、路由落池）
- `-->` 细实线 = 常规同步/异步调用
- `-.->` 虚线 = **通知 / 事件 / 可丢失**（NATS Core、心跳、遥测）
- `[( )]` 圆柱 = 存储 / 有状态组件

---

## 3. 单 Cell 内部详图（一致性域的边界）

一个 Cell 就是一个**完整的强一致域**，它内部所有领取决策都是线性一致的。

```mermaid
%%{init: {"flowchart": {"curve": "basis"}}}%%
flowchart LR
    subgraph CELL["Region Cell（强一致域 · 分片 owner）"]
        direction TB

        subgraph SCH["Scheduler / Matcher（单 Leader）"]
            MATCH["Capacity 匹配器<br/>相对有序 + 允许跳过"]
            PRIO["优先级 / 抢占仲裁"]
            QUOTA["Cell 内配额与背压"]
        end

        subgraph POOL["Task Pool · Redis Cluster（线性一致）"]
            ZP["ZSET pool:{res}<br/>待领取（优先级排序）"]
            ZD["ZSET delay<br/>延迟/定时任务"]
            ZA["ZSET active<br/>score=租约到期时间"]
            HD["HASH task:{id}<br/>任务详情"]
            LS["STRING lease:{id}<br/>NX + EX 租约"]
            DD["STRING dedup:{key}<br/>幂等去重"]
            DLQ["LIST dlq<br/>死信"]
        end

        subgraph CAP["Capacity Registry"]
            WZ["ZSET workers:{res}<br/>按剩余 capacity 排序"]
            WH["HASH worker:{id}<br/>心跳 TTL / 标签"]
        end

        REAP["Lease Reaper<br/>扫 ZSET active 超时"]
        NC["NATS Core<br/>通知 · 心跳 · 控制指令"]
        JS["JetStream R3<br/>生命周期事件流"]
    end

    W["Worker Agent ×N"]

    MATCH <--> ZP
    MATCH <--> WZ
    PRIO --> MATCH
    QUOTA --> MATCH
    ZD -.到期迁移.-> ZP
    MATCH ==Lua 原子领取==> ZA
    ZA --> LS
    REAP ==超时回收==> ZP
    ZA -.重试超限.-> DLQ

    NC -.有新任务.-> W
    W ==pull + CAS 领取==> MATCH
    W -.心跳/续租.-> LS
    W -.capacity 上报.-> WH
    W ==完成/失败==> JS
    MATCH --> JS
    W -.幂等检查.-> DD
```

### 3.1 领取为什么是「相对有序 + 可跳过」

```mermaid
%%{init: {"flowchart": {"curve": "basis"}}}%%
flowchart LR
    subgraph Z["ZSET pool:gpu（按 score 排序）"]
        T1["T1 需 GPU×8<br/>score=100"]
        T2["T2 需 GPU×4<br/>score=200"]
        T3["T3 需 GPU×2<br/>score=300"]
    end
    WK["Worker 上报<br/>剩余 GPU×4"]
    T1 -.->|"capacity 不足<br/>跳过"| SKIP["skip"]
    T2 -->|"匹配 ✓<br/>领取"| TAKE["领取 T2"]
    WK --> T1
```

> 这正是你的直觉：**T1 排在最前，但因资源不足被跳过；T2 后到却先执行。** 这个语义消息队列（Kafka/JetStream 按 offset 顺序）无法表达，ZSET + Lua 天然表达。

---

## 4. 消息层职责分工（为什么需要多种）

```mermaid
%%{init: {"flowchart": {"curve": "basis"}}}%%
flowchart TB
    subgraph L1["NATS Core（无持久化 · 可丢失 · 极低延迟）"]
        A1["新任务通知：叫醒空闲 Worker"]
        A2["Worker 心跳 / capacity 上报"]
        A3["控制指令：取消·暂停·抢占·驱逐"]
        A4["Request/Reply：健康探测·同步查询"]
        A5["进度推送 fanout → Realtime Gateway"]
    end

    subgraph L2["JetStream（持久化 · R3 强一致 · 可回溯）"]
        B1["任务生命周期事件流（审计/溯源）"]
        B2["结果可靠回传（at-least-once）"]
        B3["跨区域 Mirror：容灾与只读副本"]
        B4["延迟/定时任务的可靠触发（备选）"]
    end

    subgraph L3["Kafka / TDMQ（大吞吐管道）"]
        C1["指标 / 日志 / Trace 汇聚"]
        C2["计费流水 → 数仓"]
        C3["离线分析 / 训练数据"]
    end

    subgraph L4["Redis 数据结构（不是消息队列！）"]
        D1["任务池：待领取 / 延迟 / 执行中"]
        D2["原子领取 CAS + 租约"]
        D3["幂等去重 / 限流 / 分布式锁"]
    end

    NOTE["核心原则：<br/>状态在 L4，事件在 L2，通知在 L1，分析在 L3"]
    L1 --- NOTE
    L2 --- NOTE
    L3 --- NOTE
    L4 --- NOTE
```

| 如果丢了会怎样 | NATS Core | JetStream | Kafka | Redis 池 |
|---------------|-----------|-----------|-------|---------|
| 影响 | Worker 晚几秒轮询到 | 审计缺失、结果需补偿 | 报表延迟 | **任务丢失，不可接受** |
| 可靠性要求 | 低（有兜底轮询） | 高 | 中 | **最高（+ DB 双写）** |

---

## 5. 一致性域划分图（最关键的一张）

```mermaid
%%{init: {"flowchart": {"curve": "basis"}}}%%
flowchart TB
    subgraph STRONG["强一致域（线性一致 · Raft / 单实例原子）"]
        S1["Global Control Plane<br/>分片映射表 · Leader 选举"]
        S2["Cell A · Task Pool<br/>领取 CAS 线性一致"]
        S3["Cell B · Task Pool"]
        S4["Task Metadata DB 主库"]
    end

    subgraph EVENTUAL["最终一致域（异步复制 · 保序）"]
        E1["JetStream 跨区 Mirror"]
        E2["Redis 跨区只读副本"]
        E3["DB 只读从库"]
        E4["Kafka → 数仓"]
        E5["Cache"]
    end

    subgraph BESTEFFORT["尽力而为域（可丢失）"]
        F1["NATS Core 通知"]
        F2["心跳 / capacity 上报"]
        F3["Metrics / Traces"]
    end

    S2 -.异步.-> E1
    S3 -.异步.-> E1
    S4 -.异步.-> E3
    E1 -.-> E4

    RULE1["规则1：任何『领取/扣减/唯一性』操作<br/>必须发生在 STRONG 域内单个 Cell"]
    RULE2["规则2：跨 Cell 永不竞争同一任务<br/>由分片映射保证唯一 owner"]
    RULE3["规则3：EVENTUAL 域只读不决策<br/>灾备切换时才提升为主"]
    RULE4["规则4：BESTEFFORT 域必须有兜底<br/>通知丢失 → 定时轮询补偿"]

    STRONG --- RULE1
    STRONG --- RULE2
    EVENTUAL --- RULE3
    BESTEFFORT --- RULE4
```

---

## 6. 任务生命周期状态机

```mermaid
stateDiagram-v2
    [*] --> Accepted: 提交（幂等校验通过）
    Accepted --> Pending: 落入 pool ZSET
    Accepted --> Delayed: 指定延迟/定时
    Delayed --> Pending: 到期迁移

    Pending --> Reserved: Matcher capacity 匹配成功
    Reserved --> Dispatched: 下发 Worker + 设租约
    Dispatched --> Running: Worker 确认开始

    Running --> Running: 续租 + 进度上报
    Running --> Succeeded: 结果校验通过
    Running --> Failed: 执行异常

    Dispatched --> Pending: 租约超时回收（Reaper）
    Running --> Pending: 心跳丢失 → 回收

    Failed --> Pending: 重试（retry < max）
    Failed --> DeadLetter: 重试超限
    Running --> Cancelled: 用户取消 / 抢占
    Pending --> Cancelled: 用户取消

    Succeeded --> [*]
    DeadLetter --> [*]
    Cancelled --> [*]

    note right of Reserved
        Reserved 是短暂中间态
        必须与租约同一原子操作
        避免"预留但未下发"泄漏
    end note

    note right of Pending
        回收后重新入池
        score 可提升优先级
        防止饥饿
    end note
```

---

## 7. 场景时序图

### 7.1 场景一：任务提交 → 匹配领取 → 执行 → 进度推送（主链路）

```mermaid
sequenceDiagram
    autonumber
    participant BR as Browser
    participant GW as API Gateway
    participant API as Task Ingress API
    participant RT as Global Task Router
    participant CP as Control Plane<br/>(分片表)
    participant P as Cell-B Task Pool<br/>(Redis)
    participant JS as Cell-B JetStream
    participant NC as Cell-B NATS Core
    participant SC as Cell-B Scheduler
    participant W as Worker Agent<br/>(Region-B)
    participant OSS as Object Storage
    participant RG as Realtime Gateway

    BR->>GW: POST /tasks（含 Idempotency-Key）
    GW->>API: 鉴权后转发
    API->>API: 参数校验 + 配额检查
    API->>RT: 请求归属 Cell
    RT->>CP: 读分片映射 + 区域权重表
    CP-->>RT: 归属 Cell-B（权重最高且 capacity 足）
    RT-->>API: Cell-B

    API->>P: Lua: 幂等去重 SET dedup NX
    alt 重复提交
        P-->>API: 已存在 → 返回原 task_id
        API-->>BR: 200（幂等命中）
    else 首次提交
        P->>P: HSET task:{id} + ZADD pool:gpu
        P-->>API: OK, task_id
        API->>JS: 发布事件 task.accepted
        API-->>BR: 202 Accepted + task_id
    end

    BR->>RG: 建立 SSE 订阅 task.{id}.progress
    RG->>NC: SUB task.{id}.>

    Note over SC,NC: 调度侧异步进行
    SC->>NC: PUB worker.wake.gpu（有新任务）
    NC-->>W: 通知（可丢失，Worker 也会轮询）

    W->>SC: pull 请求（携带剩余 capacity: GPU×4, 标签, 区域）
    SC->>P: Lua 原子匹配领取<br/>扫 ZSET，跳过 capacity 不足者
    P->>P: ZREM pool + ZADD active(租约到期)<br/>SET lease:{id} NX EX 30
    P-->>SC: 命中 task T2
    SC-->>W: 下发 T2（含 OSS 输入 URL）
    SC->>JS: 事件 task.dispatched

    W->>OSS: 下载输入
    W->>NC: PUB task.{id}.progress（0%）
    NC-->>RG: fanout
    RG-->>BR: SSE: 0%

    loop 执行中
        W->>P: 续租 EXPIRE lease:{id} 30
        W->>NC: PUB task.{id}.progress（n%）
        NC-->>RG: fanout
        RG-->>BR: SSE: n%
    end

    W->>OSS: 上传输出
    W->>JS: PUB task.{id}.succeeded（持久，at-least-once）
    JS-->>SC: 消费确认
    SC->>P: Lua: ZREM active + DEL lease + 标记完成
    SC->>NC: PUB task.{id}.done
    NC-->>RG: fanout
    RG-->>BR: SSE: 100% 完成
```

### 7.2 场景二：Worker 宕机 → 租约超时 → 回收重派（可靠性核心）

```mermaid
sequenceDiagram
    autonumber
    participant W1 as Worker-1（即将宕机）
    participant P as Task Pool (Redis)
    participant RP as Lease Reaper<br/>(分片 Leader)
    participant SC as Scheduler
    participant JS as JetStream
    participant W2 as Worker-2
    participant RG as Realtime Gateway

    W1->>P: 领取 T5，SET lease:T5 = W1 EX 30
    W1->>W1: 开始执行
    W1->>P: 续租（t=10s）
    Note over W1: t=15s 宿主断电 💥

    Note over RP: 每 5s 扫描一次
    RP->>P: ZRANGEBYSCORE active 0 now
    P-->>RP: T5 租约已到期（t=40s）
    RP->>P: Lua 原子回收：<br/>校验 lease 仍属 W1 且已过期<br/>ZREM active + DEL lease<br/>retry+1 + ZADD pool（提升优先级）

    alt retry <= max
        P-->>RP: 已重回待领取池
        RP->>JS: 事件 task.reclaimed
        RP->>SC: 触发唤醒
        SC->>W2: 重新下发 T5
        W2->>P: 幂等检查 dedup:T5-attempt
        W2->>W2: 执行（业务幂等，重复执行安全）
        W2->>JS: task.succeeded
    else retry > max
        P->>P: LPUSH dlq T5
        RP->>JS: 事件 task.dead_letter
        RP->>RG: 通知前端失败
    end

    Note over RP,P: 关键：回收必须校验 lease 归属<br/>防止 W1 假死复活后重复写结果
```

### 7.3 场景三：跨区域路由 + Cell 故障转移

```mermaid
sequenceDiagram
    autonumber
    participant API as Task Ingress API
    participant RT as Global Task Router
    participant CP as Control Plane (Raft)
    participant HM as Region Health Monitor
    participant PA as Cell-A Pool（故障）
    participant JA as Cell-A JetStream
    participant JB as Cell-B JetStream（Mirror）
    participant PB as Cell-B Pool
    participant PLAN as Placement Planner

    Note over PLAN,CP: 常态：策略驱动路由
    PLAN->>CP: 更新区域权重表（Cell-B 综合评分上升）
    API->>RT: 新任务
    RT->>CP: 读分片表 + 权重
    CP-->>RT: 分片 7 → Cell-A
    RT->>PA: 落池 ✓

    Note over PA: Cell-A 整体故障 💥
    HM->>PA: 健康探测失败 ×3
    HM->>CP: 上报 Cell-A unhealthy
    CP->>CP: Raft 提案：分片 7 owner<br/>A → B（唯一决策，强一致）
    CP-->>RT: 分片表更新（watch 推送）

    Note over JB: 灾备提升
    JA-.异步 Mirror（有 lag）.->JB
    CP->>JB: 提升为主，从 Mirror 重建 Pool
    JB->>PB: 回放事件流，重建待领取任务

    RT->>PB: 新任务落 Cell-B ✓

    Note over PB: ⚠️ 关键风险点
    Note over PB: Mirror 有复制延迟 → 可能丢失<br/>Cell-A 最后几秒已领取记录<br/>→ 靠幂等 + Metadata DB 对账兜底
```

### 7.4 场景四：策略驱动的跨区放置与弹性调度

> 本节描述的是**机制**，不绑定任何具体信号口径。信号如何取值、如何加权，属于策略实现细节（§1.8）。

```mermaid
sequenceDiagram
    autonumber
    participant PC as Placement Signal Collector
    participant PLAN as Placement Planner
    participant CP as Control Plane
    participant AS as Autoscaler
    participant CB as Cell-B
    participant CC as Cell-C
    participant WB as Worker Pool B
    participant WC as Worker Pool C
    participant RT as Task Router

    loop 每个策略周期（如 5 分钟）
        PC->>PLAN: 上报各区信号<br/>{unit_cost, headroom, backlog, proximity, reliability}
        PLAN->>PLAN: ① 按 constraint 过滤出合法候选集<br/>② 候选集内加权评分 → 区域权重
        PLAN->>CP: 提交权重表（强一致写，带 version）
    end

    RT->>CP: 读权重表
    CP-->>RT: B:0.7  C:0.2  A:0.1（version=42）
    RT->>CB: 按权重分配无强时延要求的任务

    Note over AS: 弹性扩缩（同一份权重，不同决策）
    AS->>CB: 查询积压 / 等待时长
    CB-->>AS: pool:gpu 积压 1200，P95 等待 8min
    AS->>PLAN: 请求扩容位置决策
    PLAN-->>AS: 优先在 B 扩容（综合评分最高）
    AS->>WB: 扩容 +40 Worker
    AS->>WC: 缩容 -10 Worker（评分低且积压低）

    Note over RT,CC: 硬约束优先于权重
    RT->>CC: 带 latency-sensitive / data-residency 标签的任务<br/>由 constraint 过滤强制就近，<b>权重不参与决策</b>
```

**两条不可违反的规则**：

| 规则 | 理由 |
|------|------|
| **权重只影响"选哪个候选"，不影响"哪些是候选"** | 硬约束（数据驻留、专属池、时延 SLA）必须在过滤阶段处理。否则调低阈值即可绕过合规 |
| **权重表变更不得影响已入池任务的归属** | 已入池任务的 owner Cell 已确定（公理 A2）。权重仅作用于**新任务路由**与**扩缩容位置**；已入池任务的迁移是独立议题（§11.5） |

> 权重表带 `version` 单调递增：Router 缓存旧版本时可继续工作（策略陈旧只影响最优性，不影响正确性）——这是典型的「策略层可最终一致，状态层必须强一致」分层。

### 7.5 场景五：任务取消 / 抢占（控制指令下行）

```mermaid
sequenceDiagram
    autonumber
    participant BR as Browser
    participant API as Task API
    participant P as Task Pool
    participant NC as NATS Core
    participant W as Worker（正在执行）
    participant JS as JetStream
    participant RG as Realtime Gateway

    BR->>API: DELETE /tasks/{id}
    API->>P: Lua 原子判定当前状态
    alt 状态 = Pending（还在池里）
        P->>P: ZREM pool + 标记 Cancelled
        P-->>API: 已取消
        API-->>BR: 200 立即取消
    else 状态 = Running（已被领取）
        P->>P: 打取消标记 cancel:{id}=1
        P-->>API: 已投递取消指令
        API->>NC: PUB task.{id}.cancel（Request/Reply）
        NC-->>W: 收到取消
        W->>W: 中止执行 + 清理沙箱
        W-->>NC: Reply: 已中止
        W->>JS: task.cancelled
        W->>P: DEL lease + ZREM active
        JS-->>RG: 通知
        RG-->>BR: SSE: 已取消
        API-->>BR: 202 取消中
    end

    Note over NC,W: NATS Core Request/Reply 适合<br/>这类需要即时响应的控制指令<br/>丢失时 Worker 也会周期性拉 cancel 标记
```

---

## 8. 数据模型（Task Pool 的 Redis 结构）

```mermaid
%%{init: {"flowchart": {"curve": "basis"}}}%%
flowchart TB
    subgraph KEYS["Cell 内 Redis Key 结构（同一 hash slot 用 {tag} 绑定）"]
        K1["<b>task:{tid}</b> HASH<br/>payload, res_type, req_capacity,<br/>priority, retry, owner, state, ts"]
        K2["<b>pool:{cell}:{res_type}</b> ZSET<br/>score = priority×1e12 + submit_ts<br/>→ 相对有序，允许跳过"]
        K3["<b>delay:{cell}</b> ZSET<br/>score = 可执行时间戳"]
        K4["<b>active:{cell}</b> ZSET<br/>score = 租约到期时间<br/>→ Reaper 靠它 O(logN) 扫超时"]
        K5["<b>lease:{tid}</b> STRING<br/>value = worker_id + fence_token<br/>NX EX ttl"]
        K6["<b>dedup:{idem_key}</b> STRING<br/>NX EX 24h → 幂等"]
        K7["<b>workers:{cell}:{res}</b> ZSET<br/>score = 剩余 capacity"]
        K8["<b>worker:{wid}</b> HASH + TTL<br/>心跳、标签、区域、总/剩余 capacity"]
        K9["<b>cancel:{tid}</b> STRING<br/>取消标记（Worker 轮询兜底）"]
        K10["<b>dlq:{cell}</b> LIST<br/>死信"]
    end

    K2 -->|领取成功| K4
    K3 -->|到期| K2
    K4 -->|超时回收| K2
    K4 -->|重试超限| K10
    K4 --> K5
    K7 --> K8
```

### 8.1 原子领取脚本骨架（伪码）

```lua
-- KEYS[1]=pool:{cell}:{res}  KEYS[2]=active:{cell}
-- ARGV[1]=worker_id ARGV[2]=avail_capacity ARGV[3]=now ARGV[4]=lease_ttl
-- ARGV[5]=scan_limit（候选窗口，如 50）

local cands = redis.call('ZRANGE', KEYS[1], 0, ARGV[5]-1)
for i, tid in ipairs(cands) do
  local t = redis.call('HMGET', 'task:'..tid, 'req_capacity', 'labels', 'state')
  if t[3] == 'pending' and tonumber(t[1]) <= tonumber(ARGV[2])
     and match_labels(t[2], ARGV[6]) then      -- ← 关键：不匹配就 continue（跳过）
    redis.call('ZREM', KEYS[1], tid)
    redis.call('ZADD', KEYS[2], ARGV[3]+ARGV[4], tid)
    redis.call('SET', 'lease:'..tid, ARGV[1]..':'..fence, 'NX', 'EX', ARGV[4])
    redis.call('HSET', 'task:'..tid, 'state','dispatched', 'owner',ARGV[1])
    return tid                                  -- 领到即返回（相对有序）
  end
end
return nil                                      -- 无匹配任务
```

> `fence_token` 单调递增，Worker 回写结果时必须带上，Pool 校验 token 与当前 lease 一致才接受 → 防止假死 Worker 复活后覆盖新执行者的结果（**fencing**，比纯 TTL 锁安全）。

---

## 9. 依赖关系矩阵（强依赖 vs 弱依赖）

| 消费方 ↓ / 依赖 → | Control Plane | Task Pool | JetStream | NATS Core | Metadata DB | OSS |
|---|---|---|---|---|---|---|
| `Task Ingress API` | **强**（路由表） | **强** | 弱 | — | 弱（异步落库） | — |
| `Scheduler/Matcher` | **强**（Leader 选举） | **强** | 弱 | 弱 | 弱 | — |
| `Worker Agent` | — | **强**（领取/续租） | 弱（结果回传，可重试） | 弱（通知，有轮询兜底） | — | **强**（输入输出） |
| `Lease Reaper` | **强**（Leader） | **强** | 弱 | — | — | — |
| `Realtime Gateway` | — | — | 弱 | 弱（丢了可轮询） | — | — |
| `Business State Svc` | — | — | **强**（事件驱动） | — | **强** | 弱 |
| `Autoscaler` | **强** | 弱（读积压） | — | — | — | — |

**强依赖 = 挂了核心功能不可用；弱依赖 = 挂了有降级路径。**
关键结论：**只有 `Control Plane` 和 `Task Pool` 是不可降级的**，其余全部可降级——这决定了 SLO 投入的优先级。

---

## 10. 部署形态与分布式策略汇总

| 组件 | 部署形态 | 一致性 | 扩容方式 | 单点风险 |
|------|---------|--------|---------|---------|
| API Gateway / BFF | 多区域 × N 副本 | 无状态 | 水平 | 无 |
| Realtime Gateway | 多区域 × N 副本 | 连接态 | 水平 + 连接路由 | 单实例挂 → 客户端重连 |
| Task Ingress API | 多区域 × N 副本 | 无状态 | 水平 | 无 |
| **Global Control Plane** | 跨区 3/5 节点 Raft | **线性一致** | 垂直为主 | 失去多数派 → 只读降级 |
| **Scheduler/Matcher** | 每分片 1 Leader + Standby | 分片内强一致 | **增加分片数** | Leader 切换有秒级窗口 |
| **Task Pool（Redis）** | 每 Cell 一套 Cluster | 单实例原子 | 加 slot / 加 Cell | 主从切换可能丢写 → DB 对账 |
| JetStream | 每 Cell R3 + 跨区 Mirror | Cell 内强一致 | 加 Stream / 分区 | 失去 quorum → 只读 |
| NATS Core | 每区集群 + Gateway | 无状态 | 水平 | 无（可丢失） |
| Worker Agent | 全球分散，海量 | 无共享态 | 水平 + Autoscale | 无（租约回收） |
| Metadata DB | 分片 + 主从 | 主库强一致 | 分片 | 主库切换 |
| Object Storage | 托管多区 | 最终一致（对象级强） | 托管 | 低 |

---

## 11. 待讨论的开放问题（下一轮基线）

1. **分片粒度**：按 `task_id hash`、按 `tenant`、还是按 `resource_type + region`？影响热点与迁移成本。
2. **Scheduler 是「Worker pull」还是「Scheduler push」**？pull 更简单抗压，push 更好做全局最优匹配。
3. **抢占策略**：高优任务是否可抢占低优正在执行的任务？涉及检查点/续跑设计。
4. **Redis 持久化等级**：AOF everysec 是否可接受？是否需要 Task Pool 与 Metadata DB 双写 + 定期对账？
5. **跨 Cell 任务迁移**：区域权重显著下降时，已入池未领取的任务是否允许迁移到其他 Cell？（涉及跨 Cell 原子转移，与公理 A2「唯一 owner Cell」直接冲突，需专门设计两阶段移交）
6. **饥饿防护**：大 capacity 任务被反复跳过怎么办？（预留/装箱/老化提权）
7. **多租户隔离**：Cell 内是否需要按租户做公平调度（DRF / 权重轮转）？
8. ~~**Realtime Gateway 连接路由**：客户端断线重连到别的实例，如何继续收到 `task.{id}` 事件？（subject 广播 or 连接注册表）~~ → **已定论**，见 [`realtime-stream-collab.md` §5.1](./realtime-stream-collab.md)：不做连接注册表，改为「共享可回放流 + 任意实例订阅 + 客户端游标」
9. **是否真的需要 Kafka/TDMQ**：如果数仓量不大，JetStream 直出是否够用？（减少一套组件）
10. ~~**Metadata DB 与 Task Pool 的真相来源（SoT）**：谁是权威？~~ → **已定论**，见 [`task-identity-and-sot.md` §4](./task-identity-and-sot.md)：四类权威分工（运行时=Pool / 观测=事件流 / 审计=DB+OSS / 业务=业务服务），单向投影，投影层永不回写运行时权威。
