# 系统架构复验图

> **用途**：核对你与我对系统的理解是否一致。
>
> 每张图都标注了可核对的代码位置。**若图与代码不符，以代码为准并请指出**——这份文档的价值全在于它能被伪证。
>
> 核实时间：2026-09-02。基于当前工作区（未提交）状态绘制。

---

## 0. 三个最容易产生理解偏差的地方

先把它们单独列出，因为后面每张图都受其影响。

| # | 事实 | 常见误解 |
|---|---|---|
| 1 | **执行在宿主节点进程内。** 无独立执行端进程，无 `/v1/agent/*` 端点（D23） | 以为仍是外部执行端拉取 |
| 2 | **两条写路径彼此独立。** 事件流有界瞬态，内容持久 | 以为输出条目由事件流回放派生 |
| 3 | **三条转发路径中只有一条是永久的** | 把链亲和当成永久设计 |
| 4 | **领取限定本节点作用域**（§9）——生成者与在途缓冲持有者恒等 | 以为任意节点可领取任意生成 |

---

## 1. Crate 分层与依赖方向

```mermaid
graph BT
    subgraph L0["领域层（无 workspace 内依赖）"]
        core["<b>nova-responses-core</b><br/>入站协议子集 · 出站 completions 形状<br/>全部端口 trait · 领域类型 · 规范化"]
    end

    subgraph L1["适配层（实现端口）"]
        mem["<b>adapters-mem</b><br/>is_shared = false"]
        sql["<b>adapters-sql</b><br/>is_shared = true"]
        cmock["<b>adapters-completions-mock</b><br/>Echo · Scripted<br/>无模型 · 无 IO"]
        cprov["<i>adapters-completions-*</i><br/><i>真实 provider（待接入）</i>"]
    end

    subgraph L2["执行"]
        agent["<b>nova-agent</b><br/>Agent · ReAct loop<br/>零 IO"]
    end

    subgraph L3["接入层"]
        gw["<b>nova-responses-gateway</b><br/>HTTP · 路由 · 装配"]
    end

    subgraph T["验证层"]
        conf["conformance"]
        harn["harness"]
    end

    mem --> core
    sql --> core
    cmock --> core
    cprov --> core
    agent --> core
    gw --> core
    gw --> mem
    gw --> sql
    conf --> core
    conf --> mem
    harn --> conf
    harn --> sql
    gw --> agent
    gw --> cmock

    style core fill:#1a4d5c,stroke:#4db8d4,color:#fff
    style agent fill:#4a3a6b,stroke:#9b7fc7,color:#fff
    style gw fill:#5c3a1a,stroke:#d4a04d,color:#fff
    style cmock fill:#1a5c2a,stroke:#4dd47a,color:#fff
    style cprov fill:#3a3a3a,stroke:#777,color:#aaa
```

**核对点**：

- **端口在 `core`，实现在 `adapters/*`** —— `crates/core/src/ports/mod.rs` 首行即此约定。`CompletionsRequestScheduler` 现遵循它，与 `ResponseLedger` 等并列
- `core` 无 workspace 内依赖（`crates/core/Cargo.toml`）
- **`gateway` 现依赖 `nova-agent`**：执行在网关进程内（D23）。它也依赖一个 scheduler 适配器，由配置选择
- **`nova-agent` 不依赖任何具体 scheduler**：它只认端口，故换 provider 不触碰工作循环
- 以上由 `check-deps` 强制：向 `nova-agent` 注入 `reqwest` 会被拒绝（已实测验证门禁有效）

### 1.1 core 同时承载两个方向的协议，这是有意的

| 模块 | 方向 | 所有者 | 违约含义 |
|---|---|---|---|
| `protocol/` | **入站** | 我们（已发布子集） | 返回 400 |
| `completions/` | **出站** | provider | 我们的请求格式错误 |

混淆二者会双向出错：要么因为某 provider 支持而开始接受一个字段，要么因为我们的子集不含而拒绝发送一个字段。

## 2. 节点拓扑：对等无主从

```mermaid
graph LR
    C["调用方"]
    LB(["负载均衡<br/>任意节点皆可"])

    subgraph F["节点集（对等）"]
        A["<b>node-a</b> :18080"]
        B["<b>node-b</b> :18081"]
        D["<b>node-c</b> :18082"]
    end

    S[("上下文库<br/>ContextStore")]
    X["（执行在各节点进程内）"]

    C --> LB
    LB --> A
    LB --> B
    LB --> D

    A <-.->|"事件缓冲转发<br/><b>永久</b>"| B
    B <-.->|" "| D
    A <-.->|" "| D

    A --> S
    B --> S
    D --> S

    A -.->|"出站 completions"| P(["provider"])
    B -.-> P
    D -.-> P

    style A fill:#1a4d5c,stroke:#4db8d4,color:#fff
    style B fill:#1a4d5c,stroke:#4db8d4,color:#fff
    style D fill:#1a4d5c,stroke:#4db8d4,color:#fff
    style X fill:#3a3a3a,stroke:#777,color:#aaa
```

**核对点**：

- 任意节点均可创建（FR-1），无权威节点
- **在途事件缓冲是某个进程堆里的 `VecDeque`**，无共享端点可连 —— 故节点间转发是永久架构，不是过渡方案（`crates/gateway/src/routing.rs:34-42`）
- **每个节点执行自己创建的生成**（D23）。领取限定本节点作用域，故生成者与在途缓冲持有者恒等

---

## 3. 三条路由决策 —— 只有一条是永久的

这是最需要复验的一处。

```mermaid
flowchart TD
    Start(["请求到达任意节点"]) --> Q{"需要什么？"}

    Q -->|"在途事件"| R1["<b>route_inflight</b>"]
    Q -->|"已存内容"| R2["<b>route_content</b>"]
    Q -->|"创建带 previous"| R3["<b>route_chain_affinity</b>"]

    R1 --> R1D["按 node_tag 解析<br/>不看 is_shared"]
    R1D --> R1R["<b>永久转发</b><br/>缓冲在进程堆内"]

    R2 --> R2Q{"is_shared ?"}
    R2Q -->|"true（sql）"| R2L["Local<br/>直连库"]
    R2Q -->|"false（mem）"| R2P["转发到宿主节点<br/><i>临时措施</i>"]

    R3 --> R3Q{"is_shared ?"}
    R3Q -->|"true（sql）"| R3L["Local<br/>亲和失效"]
    R3Q -->|"false（mem）"| R3P["转发到上一环节点<br/><i>临时措施</i>"]

    R1R --> Unknown{"标签在<br/>对等表内？"}
    R2P --> Unknown
    R3P --> Unknown
    Unknown -->|"否"| Deny["<b>返回不存在</b><br/>绝不由标签构造地址"]
    Unknown -->|"是"| Proxy["代理转发<br/>非 307 重定向"]

    style R1R fill:#5c1a1a,stroke:#d44d4d,color:#fff
    style R2P fill:#5c4a1a,stroke:#d4c04d,color:#fff
    style R3P fill:#5c4a1a,stroke:#d4c04d,color:#fff
    style Deny fill:#5c1a1a,stroke:#d44d4d,color:#fff
```

**核对点**（`crates/gateway/src/routing.rs`）：

| 函数 | 行 | 看 `is_shared` 吗 | 性质 |
|---|---|---|---|
| `route_inflight` | 40-42 | **不看** | 永久 |
| `route_content` | 49-54 | 看 | 临时 |
| `route_chain_affinity` | 62-67 | 看 | 临时 |

**为何必须区分**：若把三者都描述为「一次定向跳转」，链亲和这个「非共享存储的权宜之计」就会固化成设计元素——长对话会把全部流量钉在单个节点上（`routing.rs:8-11` 的注释即此意）。

**换 sql 后端时，退化是自动的**：`route_content` 与 `route_chain_affinity` 因 `is_shared() == true` 直接返回 `Local`，无需改这份代码。

**SEC-5**：标签不在对等表内 → 返回不存在，且**不外发请求**。标签是攻击者可控输入，由它推导地址等于把每个 id 变成 SSRF 载体。

---

## 4. 端到端总览：多轮对话的生命周期

前三节的图各管一段；这一张把它们串起来，展示一个多轮对话从创建、执行、续接到删除的完整流转，以及**快照（D24）在其中如何固化、读取、如何被记录级删除**。

### 4.1 参与者与职责

图里最易混淆的是三个「存东西」的组件——它们存的东西不同、生命周期不同。用一张工单作类比：

| Participant | 类比 | 存什么 | 回答的问题 | 生命周期 |
|---|---|---|---|---|
| **ResponseLedger** | 工单的状态栏 | 状态（queued/in_progress/completed/failed）、attempt、幂等键、用量 | 这个 response 现在**处于什么状态、归谁**？ | 持久 |
| **ContextStore** | 工单的正文与附件 | input/output 条目 + 物化快照 | 这段对话的**历史是什么**？ | 持久（保留期可配） |
| **ResponseEventLog** | 现场的实时直播流 | 正在产生的增量事件（delta、终态） | 订阅者**此刻看到了哪些增量**？ | 瞬态（进程内、终态后释放） |
| **Agent** | 干活的工人 | 不拥有数据，只驱动流转 | **谁把活干完**？ | 进程内任务 |
| **Gateway** | 前台 | — | 请求该不该进、该不该转发 | — |
| **CompletionsRequestScheduler** | 外包渠道 | — | 怎么触达模型 | — |
| **ToolExecutor** | 工具间 | — | 模型要调的工具怎么落地 | — |
| **Provider** | 模型服务 | — | — | — |
| **Caller** | 调用方 | — | — | — |

一句话记住三者：**ResponseLedger 管「状态」，ContextStore 管「历史」，ResponseEventLog 管「正在发生的增量」**；Agent 是协调这三者的「工人」，自己不留数据。

**一个 response 就是一个 agent 的完整执行**：Agent 内部跑 ReAct 循环——模型要工具就调 `ToolExecutor`，把结果喂回模型再继续，直到模型给出最终答案。工具调用与结果既进快照（下一轮模型能看到完整轨迹），也作为 `output_item.*` 事件流式推送给订阅者。

```mermaid
sequenceDiagram
    autonumber
    participant C as Caller
    participant GW as Gateway
    participant LG as ResponseLedger
    participant CX as ContextStore
    participant EV as ResponseEventLog
    participant EN as Agent
    participant SC as Scheduler
    participant TE as ToolExecutor
    participant PR as Provider

    rect rgba(26, 77, 92, 0.2)
    Note over C,PR: 阶段一 · 首轮：无历史，直接生成
    C->>GW: POST /v1/responses
    GW->>LG: create(record)
    LG-->>GW: Accepted
    GW->>CX: put(record)
    GW->>EV: append(Created, seq=0)
    GW->>EN: notify_work()
    GW-->>C: 202 / SSE / wait terminal

    EN->>LG: claim(node_tag)
    LG-->>EN: ClaimedResponse attempt
    EN->>EV: append(InProgress)
    EN->>CX: get(record) 空快照
    EN->>EN: beginReActLoop
    loop ReActLoop（≤ max_tool_rounds）
        EN->>EN: build CompletionsRequest
        EN->>SC: schedule(request, sink)
        SC->>PR: outbound call
        loop 增量
            PR-->>SC: text chunk
            SC-->>EN: text_delta
            EN->>EV: append(delta, attempt)
        end
        PR-->>SC: done
        SC-->>EN: CompletionsOutcome{finish}
        alt finish = ToolCalls
            EN->>TE: call(name, args)
            TE-->>EN: output
            EN->>EV: append(output_item.added / done, function_call_output)
        else finish = Stop / Refusal / Length
            Note over EN: 退出循环
        end
    end
    EN->>LG: complete(attempt, status, usage)
    EN->>CX: append_output(call + output + answer)
    EN->>EV: append(terminal, attempt=None)
    EN->>EV: close(retain_ms)
    end

    rect rgba(26, 77, 92, 0.2)
    Note over C,PR: 阶段二 · 次轮：带 previous，快照固化后生成
    C->>GW: POST /v1/responses previous=首轮
    GW->>CX: resolve_chain(previous)
    CX-->>GW: 扁平快照
    GW->>LG: create(record.context = segments)
    LG-->>GW: Accepted
    GW->>CX: put(record 含快照)
    GW->>EV: append(Created)
    GW->>EN: notify_work()
    GW-->>C: 202

    EN->>LG: claim(node_tag)
    EN->>CX: get(record) 读已固化快照
    Note right of EN: 不再回溯 previous<br/>祖先缺失不影响本环
    Note right of EN: 同样跑 ReAct loop（见阶段一）<br/>工具调用与结果一并进快照
    EN->>SC: schedule(含完整历史)
    SC->>PR: outbound call
    PR-->>SC: done
    SC-->>EN: CompletionsOutcome
    EN->>LG: complete
    EN->>CX: append_output
    EN->>EV: append(terminal) + close
    end

    rect rgba(92, 26, 26, 0.2)
    Note over C,PR: 阶段三 · 删除：记录级，下游存活
    C->>GW: DELETE /v1/responses/首轮
    GW->>CX: delete(首轮) 只删记录
    Note right of CX: 次轮快照不变<br/>继承的副本原样保留
    CX-->>GW: deleted
    GW-->>C: 200

    C->>GW: 订阅次轮续订
    GW->>CX: resolve_chain(次轮)
    CX-->>GW: 完整历史，含首轮内容
    end
```

**这张图要传达的三件事**：

1. **两条写路径从不交汇**：增量走 `ResponseEventLog`（瞬态），输出条目走 `ContextStore`（持久）。`Agent` 是唯一同时触碰两者的组件，但它把「流式给订阅者看」和「终态提交存储」作为两次独立写入。
2. **快照在创建时固化、执行时读取**：阶段二里，`previous` 的历史在 `create` 那一刻被解析成扁平快照，随后执行只是单次读取——这就是「祖先缺失不影响本环」的由来（D24）。
3. **删除是记录级的，下游存活**：阶段三里删除首轮只删那条记录，次轮快照里的扁平副本原样保留，次轮仍可解析（完整历史），而非像旧链式方案那样整链断裂。

---

## 5. 创建时序（三种投递模式）

```mermaid
sequenceDiagram
    participant C as 调用方
    participant G as 网关（任意节点）
    participant L as ResponseLedger
    participant S as ContextStore
    participant E as ResponseEventLog

    C->>G: POST /v1/responses

    rect rgba(92, 26, 26, 0.25)
    Note over G: 准入检查（顺序有意义）
    G->>G: 1 租户校验
    G->>G: 2 draining？→ 503
    G->>G: 3 preflight 定向补救说明
    G->>G: 4 严格解析（未知字段→400）
    G->>G: 5 validate（规模/URL/条目类型）
    end

    opt 带 previous_response_id
    G->>G: 6 链亲和路由 → 可能代理转发
    end

    rect rgba(26, 77, 92, 0.3)
    Note over G,S: 7 先解析前驱，后创建
    G->>S: resolve_chain(tenant, previous, limits)
    S-->>G: 扁平快照 / ChainBroken·TooLong·NotStored
    Note right of G: 快照固化前的断裂检查（D24）<br/>必须在创建前失败，否则留下半创建记录
    end

    G->>L: 8 create(record, idempotency_key)
    alt Accepted
        L-->>G: Accepted
    else Duplicate
        L-->>G: Duplicate{原 id}
        G-->>C: 返回原生成（绝不产生第二个）
    else ReadOnly / Overloaded
        L-->>G: 降级 / 过载
        G-->>C: 503 / 429
    end

    opt store == true（默认）
    G->>S: 9 put(record)
    end

    G->>E: 10 append(Created, seq=0)
    Note right of E: 立即订阅者也能看到确定起点

    G->>G: 11 notify_work()
    Note right of G: 交给<b>本节点</b> Agent（D23）。<br/>本节点持有该生成的在途缓冲，<br/>是唯一能让订阅者看到增量的节点。<br/>通知而非轮询：同步模式等终态，<br/>轮询间隔会直接加到首字延迟

    alt stream=true
        G-->>C: 12a SSE 流（同连接）
    else background=true
        G-->>C: 12b 202 + 生成对象
    else 默认（同步）
        G->>G: 12c 等终态，超时返回当前状态供轮询
        G-->>C: 生成对象
    end
```

**核对点**（`crates/gateway/src/routes/responses.rs:53-264`）：

- **步骤 7 在 8 之前**是刻意的：链断裂若发生在创建之后，会留下一条永不可用的半创建记录
- 幂等重放返回**原生成**，不产生第二个（`responses.rs:206-213`）
- `Created` 事件 `seq=0`，是「0 基连续」的起点（`responses.rs:238-247`）
- 三模式共用**同一条内部事件流**
- **执行由创建它的节点承担**，不是任意节点（§6、§9）

---

## 6. 执行时序（内部执行 + 栅栏）

```mermaid
sequenceDiagram
    participant C as 调用方
    participant G as 网关 HTTP
    participant E as 本节点 Agent
    participant L as Ledger
    participant S as ContextStore
    participant B as 本节点在途缓冲
    participant T as ToolExecutor
    participant P as provider

    C->>G: POST /v1/responses
    G->>L: create
    G->>B: append(Created, seq=0)
    G->>E: notify_work()
    Note right of E: 通知而非轮询：<br/>同步模式等终态，<br/>轮询间隔会直接加到首字延迟
    G-->>C: 202 / SSE / 等终态

    E->>L: claim(<b>本节点标签</b>, agent, ttl)
    Note over L: 单点原子转换，attempt 递增<br/><b>只取本节点的生成</b>（FR-4）
    L-->>E: ClaimedResponse{attempt}
    E->>B: append(InProgress, attempt)

    E->>S: get(record) → 读 context 快照（D24）
    Note right of E: 历史已物化，单次读，<br/>不再回溯；快照自洽，<br/>祖先缺失不影响执行

    E->>E: beginReActLoop
    loop ReActLoop（≤ max_tool_rounds）
        E->>E: CompletionsRequest::from_context
        E->>P: scheduler.schedule(request, sink)

        loop 增量
            P-->>E: text_delta
            E->>B: append(delta, attempt)
            alt attempt 已被抬高
                B-->>E: StaleAttempt
                Note over E: SinkVerdict::Stop<br/>立即放弃，不再耗费 token
            end
        end

        P-->>E: CompletionsOutcome{items, usage, finish}
        E->>E: validate_outcome
        Note right of E: 不可存的结果在此拒绝，<br/>日志点名是哪个 scheduler

        alt finish = ToolCalls
            E->>T: call(name, args)
            T-->>E: output
            E->>B: append(output_item.added / done, function_call_output)
        else finish = Stop / Refusal / Length
            Note over E: 退出循环
        end
    end

    E->>L: complete(attempt, status, usage)
    opt record.stored
    E->>S: <b>append_output(call + output + answer)</b>
    Note right of S: 第二条独立写入路径<br/>非事件流回放
    end
    E->>B: append(终态事件, attempt=None)
    E->>B: close(retain_ms)
```

**核对点**（`crates/agent/src/engine.rs`、`crates/gateway/src/execution.rs`）：

- **`claim` 携带本节点标签**。这是 D23 的核心：只有本节点持有该生成的在途缓冲
- 上下文由**创建时固化**的快照提供（D24）：Agent 单次读取，scheduler 无租户上下文，不得自行解析
- 栅栏保留：`append` 携带 attempt，被取代的持有者写入返回 `StaleAttempt` → `SinkVerdict::Stop`
- **终态事件 `attempt: None`**：栅栏已由 ledger 校验，再校验会拒掉宣告转换的那条事件，流将永不终止
- 并发上限由 `max_concurrent_executions` 约束**本进程**；provider 侧限流属 scheduler

### 5.1 保留 attempt 栅栏的理由

内部执行让并发双领在结构上难以发生，但栅栏**不可删除**：执行任务卡死超过执行上界会被清扫器回收；若该任务此后苏醒并追加，其 attempt 已过期，必须被拒（FR-6 / CR-7 / INV-6）。

> 「内部执行后不会双领，故可去掉 attempt」是错误推论：卡死任务的苏醒写入与回收是**并发**的，与领取无关。

## 7. 两条写路径（最关键的一张）

```mermaid
graph TB
    X["本节点 Agent 产出"]

    subgraph P1["路径 1：增量事件"]
        E1["EventLog.append"]
        E2["进程内 VecDeque<br/><b>有界 · 瞬态</b>"]
        E3["按 retain_ms 过期<br/>过期后 410，无恢复路径"]
    end

    subgraph P2["路径 2：持久内容"]
        S1["ContextStore.append_output"]
        S2["共享存储<br/><b>持久 · 权威</b>"]
        S3["按 expires_at 清理<br/>可作为下一环"]
    end

    X -->|"流式给调用方看"| E1
    X -->|"终态显式提交"| S1
    E1 --> E2 --> E3
    S1 --> S2 --> S3

    E2 -.->|"<b>禁止</b>：回放派生输出"| S2

    style E2 fill:#5c4a1a,stroke:#d4c04d,color:#fff
    style S2 fill:#1a5c2a,stroke:#4dd47a,color:#fff
```

**核对点**：

- 两条路径由 Agent **分别写入**，无派生关系
- 若输出条目由事件流回放派生，则持久历史将依赖一个随时可被驱逐的有界缓存
- 已由 L0 `output-provenance` 用例验证：**销毁事件流后，已存输出必须依然完整**（`testing/conformance/src/lib.rs` 的 `assert_output_provenance`）

**这条约束的实际后果**：节点被 SIGKILL 时，在途事件缓冲随进程消失，对端明确失败（502）而非编造部分数据；但已完成的历史完好无损。这是分层的代价与收益的交换点。

---

## 8. 订阅与续订

```mermaid
sequenceDiagram
    participant C as 调用方
    participant N as 任意节点
    participant H as 宿主节点
    participant E as 宿主的事件缓冲

    C->>N: GET /v1/responses/{id}?stream=true&starting_after=N
    N->>N: route_inflight(id) 按 node_tag

    alt 标签不在对等表
        N-->>C: 404（不外发请求）
    else 本节点即宿主
        N->>E: read_after(cursor)
    else 宿主是对端
        N->>H: 代理转发（非 307）
        Note over N,H: 重定向会泄露内部拓扑<br/>且调用方可能无路由
        H->>E: read_after(cursor)
    end

    alt 位点已被驱逐
        E-->>H: Expired
        H-->>C: <b>410</b>
        Note over C: 不返回部分数据<br/>不静默换位点<br/>无恢复路径
    else 正常
        E-->>H: 事件批次
        H-->>C: SSE：seq 连续，游标排他
        Note over C: 唯一需持久化的状态<br/>= (response_id, seq)
    end

    Note over C,E: 见到终态事件 → 流结束
```

**核对点**（`crates/gateway/src/sse.rs`、`routes/responses.rs:355-420`）：

- `starting_after` 是**排他**游标：`starting_after=0` 跳过 seq 0
- 410 是硬失败（`sse.rs:37`）：不返回部分数据、不换位点、无恢复路径
- 调用方唯一需持久化的连接级状态是游标 → 换连接、换设备、换节点都能续订（FR-11/FR-32）

---

## 9. Agent 与出站调度：两个相反的方向

```mermaid
graph LR
    subgraph GWP["网关进程（一个节点）"]
        HTTP["HTTP 路由<br/>/v1/responses"]
        ENG["<b>Agent</b><br/>何时领活 · 并发上限<br/>栅栏 · 失败归类<br/>ReAct loop"]
        BUF[("在途缓冲<br/>本进程堆")]
    end

    subgraph CORE["core"]
        PORTS["Ledger · EventLog<br/>ContextStore"]
        REQ["<b>CompletionsRequest</b>"]
        SCH["<b>CompletionsRequestScheduler</b><br/>trait · 出站边界"]
        TOOL["<b>ToolExecutor</b><br/>trait · 工具出站边界"]
    end

    subgraph AD["适配层"]
        M1["EchoScheduler"]
        M2["ScriptedScheduler"]
        M3["真实 provider<br/><i>待接入</i>"]
    end

    P(["provider"])

    HTTP -->|"notify_work"| ENG
    ENG --> PORTS
    ENG --> BUF
    ENG --> REQ
    ENG -->|"finish=ToolCalls 时"| TOOL
    REQ --> SCH
    SCH -.-> M1
    SCH -.-> M2
    SCH -.-> M3
    M3 --> P

    style GWP fill:#5c3a1a,stroke:#d4a04d,color:#fff
    style CORE fill:#1a4d5c,stroke:#4db8d4,color:#fff
    style M3 fill:#3a3a3a,stroke:#777,color:#aaa
```

### 8.1 为何 Agent 与 scheduler 不能合并

| 若合并方向 | 后果 |
|---|---|
| 领活循环并入 scheduler 端口 | 每个 provider 适配器都要重新实现领取、栅栏、失败归类 |
| provider 关切放进 Agent | 限流成为**集群形状的属性**，换 provider 就要重新调部署 |

职责切分：

- **Agent 决定**：何时领活、本进程并发上限、失败是否终结响应
- **scheduler 决定**：一切与触达模型有关的事，**包括排队与限流**

### 8.2 命名为 Scheduler 的实际后果

`CompletionsRequestScheduler` 比 `Executor` 更宽——允许实现内部做排队、限流、批处理、连接池复用、跨 provider 重试。出站边界确实需要这些。

由此产生的规矩：**provider 侧并发策略属于端口之后**。`SchedulerError` 因此区分 `Refused`（队列已满，**未发出**，重试不额外花钱）与 `Unavailable`（已发出并耗费）。

### 8.3 命名与结构变更对照

| 原 | 现 | 理由 |
|---|---|---|
| `ChatTask` | `CompletionsRequest`（在 `core::completions`） | 实际发出的是 completions 请求；「chat」是更上层语义，由响应与上下文链承载 |
| `ChatExecutor` | `CompletionsRequestScheduler`（在 `core::ports`） | 见 8.2；且按约定端口归 `core`、实现归 `adapters/*` |
| `AgentGateway` + `ExecutionWorker` | `Agent`（`crates/agent`） | 拉取协议取消（D23），控制面 trait 一并消失；Agent 内嵌 ReAct loop |
| `mock-agent`（二进制） | **删除** | 其唯一职责是拉取协议客户端 |
| `testing/sim` | **删除** | D20 前的 Home/Edge 模拟器，已失效 |

## 10. 后台维护与优雅停机

```mermaid
graph TB
    subgraph SW["sweeper：单循环三件事（2s 一跳）"]
        T1["1 reap 失联 claim<br/>抬高 attempt 栅栏<br/>部分用量由 ledger 自身记账"]
        T2["2 释放过期事件缓冲"]
        T3["3 清理过期内容"]
    end

    subgraph DR["SIGTERM → drain"]
        D1["accepting = false"]
        D2["创建 → 503 draining"]
        D5["<b>Agent 停止取新活</b><br/>在途继续跑完"]
        D3["<b>读与订阅继续服务</b>"]
        D4["在途完成后退出"]
    end

    style T1 fill:#1a4d5c,stroke:#4db8d4,color:#fff
    style D3 fill:#1a5c2a,stroke:#4dd47a,color:#fff
    style D5 fill:#1a4d5c,stroke:#4db8d4,color:#fff
```

**核对点**：

- 三件事合并为一个循环（`crates/gateway/src/sweeper.rs:1-5`）：三个独立循环意味着三个定时器和三次忘记其一的机会
- 部分用量在回收时由 ledger 自身记账（`sweeper.rs:43-45`），故两步之间崩溃不会丢失（INV-51）
- drain 期间**读与订阅继续服务**（`state.rs` 的 `accepting` 注释）——这是滚动发布不中断在途流的原因
- 同一个标志也让 **Agent 停止领取新活**（`execution.rs:76-79`）：在途生成跑完，新的不再开始。这是内部执行后新增的一处耦合——若 Agent 不看该标志，drain 期间它仍会不断取活，节点永远等不到退出

---

## 11. 需要你确认的判断

### 已解决（按时间倒序）

**补上 Agent 层：一个 response 就是一个 agent 的 ReAct 执行。** 原先 `ExecutionEngine` 只做单次 completions 调用，拿到 outcome 即 complete，完全忽略 `FinishReason::ToolCalls`——模型请求工具时竟被当作最终答案提交。现改名为 `Agent` 并内嵌 ReAct 循环，新增 `ToolExecutor` 端口承载工具调用：

```
crates/core/src/ports/tool.rs   ← ToolExecutor trait · ToolError · NoopToolExecutor
crates/agent/src/engine.rs      ← Agent：claim → ReAct loop → complete
```

- 工具调用与结果（`FunctionCall`/`FunctionCallOutput`）一并物化进快照（D24），下一轮模型可见完整轨迹
- `max_tool_rounds` 把永不终止的工具循环截断为 Incomplete
- 工具执行失败（`ToolError`）→ 响应 Failed

**D23 · 执行改为内部执行。** 拉取协议依赖一个从未写明的前提——**领取方即宿主节点**。内存后端各持自有账本，该前提免费成立；账本变共享（D21）后前提失效且无任何机制表达：连着 node-a 的执行端可领走 node-b 的生成，增量落入 node-a 缓冲，而订阅者按 id 内的 node_tag 被送往 node-b，只看到 `Created` 随后静默，**任何路径都不报错**。

改动：

```
删除  /v1/agent/{claim,heartbeat,append,complete}
删除  testing/mock-agent（唯一职责是拉取客户端）
删除  testing/sim（D20 前 Home/Edge 模拟器，已失效）
新增  ResponseLedger::claim 携带 NodeTag；sql 加 node_tag = $3 谓词
新增  Agent（crates/agent），由网关进程内驱动
新增  L0 用例 claim-locality（已变异验证会失败）
新增  check-deps 门禁：拉取端点不得回归、claim 必须节点作用域
```

`spec.md` FR-4/5 已同步改写。

**端口与实现的归属已纠正。** 原先 `ChatExecutor` 及其实现全在 `crates/agent`，违背 `ports/mod.rs` 明文的「实现位于 `crates/adapters/*`」。现为：

```
crates/core/src/ports/completions.rs       ← 端口 trait
crates/core/src/completions/               ← 出站类型 + 翻译
crates/adapters/completions-mock/          ← Echo · Scripted · Hang
crates/agent/src/engine.rs                 ← Agent（ReAct loop）
```

**命名已按意见调整**，见 §9.3。

### 仍待确认

**④ 真实 provider 适配器尚未实现**（按「具体实现后置」）。接入时新增一个 `adapters/completions-<provider>` crate 实现端口即可，工作循环与转换均不动。需要你定的：

| 决策 | 我的建议 |
|---|---|
| base_url / api_key 来源 | **仅环境变量**（SEC-4 已要求密钥不入配置文件与日志） |
| SSE 增量解析 | 放适配器内；worker 只见 `text_delta` |
| 超时与重试 | 放**适配器内**（§9.2：并发策略属端口之后） |
| 限流 | 同上。`SchedulerError::Refused` 已为「未发出即拒」预留 |

**⑤ `ResponseLedger` 缺少读取部分用量的方法。** `partial_usage_count` 只在 mem 具体类型上，不在 trait 内，故只持有端口的计费方取不到已记账金额。CR-11 目前由 L1 经 trace 覆盖，而非端口契约。若计费确需经端口取数，这是一处真实缺口。

**⑥ FR-31 是唯一延后需求**，需真实数据库验证「共享存储后不再转发」。

## 附：与文档的对应关系

| 本文档章节 | 权威来源 |
|---|---|
| 1 Crate 分层 | 各 `Cargo.toml` 的 `[dependencies]` |
| 2 节点拓扑 | `docs/architecture/arc.md` |
| 3 三条路由 | `crates/gateway/src/routing.rs:1-15`（模块注释即设计依据） |
| 4 端到端总览 | 本文档 §5/§6/§8 的综合 · `decisions.md` D24 |
| 5 创建时序 | `docs/design/01-responses-api.md` · `routes/responses.rs:53-264` |
| 6 执行时序 | `crates/agent/src/engine.rs` · `crates/gateway/src/execution.rs` |
| 7 两条写路径 | `docs/architecture/invariants.md` INV-48 · `docs/design/03-context-chain.md` |
| 8 订阅续订 | `crates/gateway/src/sse.rs` |
| 9 执行与调度 | `crates/agent/src/lib.rs` 模块注释 · `decisions.md` D23 |
| 10 维护与停机 | `docs/design/05-reliability.md` · `sweeper.rs` |
| 协议子集 | `docs/design/06-protocol-subset.md` |

### 一处文档不一致（复验发现）

`docs/design/01-session-stream.md` 整篇描述 **Session / Turn / 快照开屏 / 热层冷层 / 跨区镜像**——这套概念已在本次重构中移除（D20），对应的 `/v1/sessions/*` 端点与 `trim_hot` 也已删除。但 `docs/design/README.md` 仍将其列为现行设计。

该文档已标注为历史状态。保留而非删除，是因为它记录了被取代的方案，对理解 D20 为何这样收口有价值；但它**不描述当前系统**。
