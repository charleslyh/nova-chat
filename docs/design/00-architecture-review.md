# 系统架构复验图

> **用途**：核对你与我对系统的理解是否一致。
>
> 每张图都标注了可核对的**符号位置**（模块、函数、类型名），而非行号——行号会随任何一次编辑失效，而失效的引用比没有引用更糟。**若图与代码不符，以代码为准并请指出**：这份文档的价值全在于它能被伪证。
>
> 核实时间：2026-09-08，基于当前工作区状态。架构基线为 **D25 + D28**（执行进程独立 + 在途缓冲共享化 + 能力层抽离 + claim 全局化 + 会话/轮次领域）。
>
> **本文档不记录决策推导**。为什么这样选、否决了什么，唯一权威是 [`decisions.md`](../architecture/decisions.md)；此处只描述「现在长什么样」。

---

## 0. 五个最容易产生理解偏差的地方

先单独列出，因为后面每张图都受其影响。

| # | 事实 | 常见误解 |
|---|---|---|
| 1 | **执行是独立进程 `nova-agentd-mock`**，经 `ResponseLedger` 端口直连共享账本领活 | 以为执行在网关进程内，或以为执行经 HTTP 拉取 |
| 2 | **claim 是全局的**：任意执行进程可领取任意 queued 生成 | 以为领取限定「创建它的那个节点」 |
| 3 | **两条写路径彼此独立**：增量走事件缓冲（有界瞬态），输出条目走上下文库（持久） | 以为输出条目由事件流回放派生 |
| 4 | **没有节点间转发**：存储是共享载体，任意节点直读 | 以为仍有节点间转发 |
| 5 | **唯一运行时载体是 mem**：执行分离为独立 `nova-agentd-mock`，共享载体是 `nova-responses-mem-server`（进程内数据 + HTTP 数据面/控制面） | 以为 mem 仍单进程内嵌执行 |

第 5 条最容易漏，它是理解 §1 与 §4 的前提。

---

## 1. 唯一载体，多进程拓扑

运行时只有一种形态：共享载体是 `nova-responses-mem-server`（进程内数据 + HTTP 数据面 + 控制面）。gateway 直接挂 `adapters-mem-client` 客户端桩，无编译期后端选择。

| | 运行时（唯一形态） |
|---|---|
| 账本 / 上下文库 | `adapters-mem-client` → `mem-server` |
| 在途事件缓冲 | `adapters-mem-client` → `mem-server` |
| 会话（conversation，D28） | `adapters-mem-client` → `mem-server` |
| 执行位置 | **独立进程** `nova-agentd-mock` |
| 维护（sweep） | **独立进程** `nova-responses-sweep` |
| 时钟 | `SystemClock`（真实墙钟） |
| 用途 | 协议验证 · 本地开发 · L0–L2 · L4 |

**进程拓扑**：gateway（HTTP 接入）+ `nova-agentd-mock`（执行）+ 独立 sweep（reap/过期清理）+ 独立载体。执行是独立进程，不内嵌于 gateway。

**mem 载体如何跨进程**：`adapters-mem` 拆成两半——数据本体（`MemWorld`）留在 `nova-responses-mem-server` 进程，对外经 `proto`/`server` 模块暴露 `POST /rpc` 数据面（另有 `/unavailable`、`/tamper`、`/advance_clock`、`/set_clock` 控制面，仅供测试注入故障）；`adapters-mem-client` 是实现了四个端口（`ResponseLedger`/`ResponseEventLog`/`ContextStore`/`ConversationStore`）的 RPC 桩，gateway/agentd/sweep 各自持有一份，连同一个 `mem-server`。

**核对点**：

- 载体数据面：`crates/adapters/mem/src/proto.rs`（`Request`/`Response`）与 `server.rs`（`dispatch`）；HTTP 装配在 `crates/mem-server/src/main.rs`（数据面 `/rpc` + 控制面 `/unavailable` `/tamper` `/advance_clock` `/set_clock`）
- 客户端桩：`crates/adapters/mem-client/src/{ledger,event_log,context,conversation}.rs`
- 执行：`crates/agentd/src/main.rs`（`mount_mem`，`--scheduler echo|scripted|http`）
- 独立 sweep：`crates/sweep/src/main.rs`，复用 `nova-responses::sweeper::SweepDeps`
- 能力层 / HTTP 层：都在 `nova-responses` library
- 夹具：`testing/config/node-{a,b,c}.toml` 供 mem gateway 用；`xtask` 的 `procs up` 启动 mem-server + agentd + sweep + 三个 gateway

---

## 2. Crate 分层与依赖方向

```mermaid
graph BT
    subgraph L0["领域层（无 workspace 内依赖）"]
        core["<b>nova-responses-core</b><br/>入站协议子集 · 出站 completions 形状<br/>全部端口 trait（Ledger · EventLog · Context · Conversation ·<br/>CompletionsScheduler · ToolExecutor · Integrity · Metrics · Clock）<br/>领域类型 · 规范化"]
    end

    subgraph L1["适配层（实现端口）"]
        mem["<b>adapters-mem</b><br/>数据本体 · proto · server"]
        memclient["<b>adapters-mem-client</b><br/>RPC 桩（数据面）"]
        cmock["<b>adapters-completions-mock</b><br/>Echo · Scripted<br/>无模型 · 无 IO"]
        chttp["<b>adapters-completions-http</b><br/>真实 chat-completions（HTTP）"]
        tool["<b>adapters-tool-calculator</b><br/>ToolExecutor 实现"]
    end

    subgraph L2["执行"]
        agent["<b>nova-agent-runtime</b><br/>Agent · ReAct loop<br/>零 IO"]
    end

    subgraph SRV["服务层（无具体 adapter 依赖）"]
        svc["<b>nova-responses</b><br/>能力层 · HTTP 层 · 后台维护<br/>sweeper · 优雅停机"]
    end

    subgraph L3["二进制"]
        gw["<b>nova-responses-gateway</b><br/>薄装配"]
        agentd["<b>nova-agentd-mock</b><br/>执行进程"]
        memsrv["<b>nova-responses-mem-server</b><br/>共享载体（数据+控制面）"]
        sweep["<b>nova-responses-sweep</b><br/>独立维护进程"]
    end

    subgraph T["验证层"]
        conf["conformance<br/>L0 端口契约"]
        harn["harness<br/>L1 / L2"]
        sdk["sdk-compat<br/>L4（Python SDK）"]
    end

    mem --> core
    memclient --> core
    memclient --> mem
    cmock --> core
    chttp --> core
    tool --> core
    agent --> core
    svc --> core
    agentd --> agent
    agentd --> memclient
    agentd --> cmock
    agentd --> chttp
    agentd --> tool
    gw --> svc
    gw --> memclient
    memsrv --> mem
    sweep --> memclient
    sweep --> svc
    conf --> core
    conf --> mem
    harn --> conf
    harn --> svc
    harn --> mem
    sdk -.->|"HTTP"| gw

    style core fill:#1a4d5c,stroke:#4db8d4,color:#fff
    style agent fill:#4a3a6b,stroke:#9b7fc7,color:#fff
    style agentd fill:#4a3a6b,stroke:#9b7fc7,color:#fff
    style svc fill:#2a5c2a,stroke:#4dd47a,color:#fff
    style gw fill:#5c3a1a,stroke:#d4a04d,color:#fff
    style cmock fill:#1a5c2a,stroke:#4dd47a,color:#fff
    style chttp fill:#3a3a3a,stroke:#777,color:#aaa
```

**核对点**：

- **端口在 `core`，实现在 `adapters/*`** —— `core/src/ports/mod.rs` 首行即此约定。`CompletionsRequestScheduler`、`ToolExecutor`、`ConversationStore`、`ContentIntegrity`、`MetricsSink` 都与 `ResponseLedger` 并列
- `core` 无 workspace 内依赖（`crates/core/Cargo.toml`），这是 `check-deps` 的不变量
- **`nova-agent-runtime` 不依赖 HTTP / DB / 任何具体 scheduler**：`check-deps` 拒绝向它注入 `reqwest`/`hyper`/`axum`/`sqlx`（已实测门禁有效）。因此 claim → ReAct → submit 全路径可在无 socket、无模型的单测里跑完
- **gateway 不依赖 `nova-agent-runtime`**：执行统一走独立进程 `nova-agentd-mock`，gateway 只做接入与投递
- `adapters-completions-mock` 不得依赖 `reqwest`/`sqlx`/`nova-agent-runtime`（同门禁），否则一个测试可能悄悄发出真实调用

### 2.1 core 同时承载两个方向的协议，这是有意的

| 模块 | 方向 | 所有者 | 违约含义 |
|---|---|---|---|
| `protocol/` | **入站** | 我们（已发布子集） | 返回 400 |
| `completions/` | **出站** | provider | 我们的请求格式错误 |

混淆二者会双向出错：要么因为某 provider 支持而开始接受一个字段，要么因为我们的子集不含而拒绝发送一个字段。

---

## 3. 协议表面

全部端点，来自 `nova-responses/src/routes/mod.rs` 的 `router()`：

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/health` | 存活 + `accepting` 状态 |
| `POST` | `/v1/responses` | 创建（三种投递模式） |
| `GET` | `/v1/responses/{id}` | 检索当前状态对象；`?stream=true` 转 SSE 订阅 |
| `DELETE` | `/v1/responses/{id}` | 记录级删除 |
| `POST` | `/v1/responses/{id}/cancel` | 取消在途生成 |
| `GET` | `/v1/conversations` | 列出会话（D28） |
| `POST` | `/v1/conversations` | 创建会话（D28） |
| `GET` | `/v1/conversations/{id}` | 检索会话（D28） |
| `POST` | `/v1/conversations/{id}` | 更新 metadata（上游用 POST 到同路径，D28） |
| `DELETE` | `/v1/conversations/{id}` | 删除会话（不级联删响应，D28） |
| `GET` | `/v1/conversations/{id}/events` | 会话事件流 SSE（D28） |
| `POST` | `/v1/conversations/{id}/events` | 追加业务事件（D28） |
| `GET` | `/v1/conversations/{id}/transcript` | 全量历史（D28） |
| `POST` | `/v1/admin/read_only` | 降级只读 |
| `POST` | `/v1/admin/pending_limit` | 过载阈值 |
| `POST` | `/v1/tenants/{tenant}/purge` | 租户清除 |

执行不是协议：`nova-agentd-mock` 经 `ResponseLedger` 端口领活，不经 `router()` 注册的 HTTP 端点。

`/v1/conversations` 是 D28 引入的会话容器：CRUD 对齐上游，`/events`、`/transcript` 是自托管子资源（上游无对应协议）。会话**不存条目**——它只存链尾指针 + 轮次锁 + 事件流，上下文仍由 `resolve_chain` 单一入口装配。

`GET /v1/responses/{id}` 未完成时返回 `status: in_progress` 的**部分对象而非失败**——这是 `background=true` 轮询机制的语义基石（详见 [`06-protocol-subset.md`](./06-protocol-subset.md) §7）。

---

## 4. 无节点间转发

存储是共享载体（`nova-responses-mem-server`），任意节点直读，因此**不存在**节点间转发。

核对点：`nova-responses/src/routes/responses.rs` 的 retrieve / stream / cancel / delete 委托给 `service` 层（`state.service`），由 service 直接读 `ledger` / `context` / `event_log` 端口（背后是共享的 `mem-server`）。

---

## 5. 端到端总览：多轮对话的生命周期

前几节各管一段；这一张把它们串起来，展示一个多轮对话从创建、执行、续接到删除的完整流转，以及**快照（D24）如何固化、读取、如何被记录级删除**。

### 5.1 参与者与职责

最易混淆的是三个「存东西」的组件——它们存的东西不同、生命周期不同。用一张工单作类比：

| 参与者 | 类比 | 存什么 | 回答的问题 | 生命周期 |
|---|---|---|---|---|
| **ResponseLedger** | 工单的状态栏 | 状态、attempt、幂等键、用量 | 这个 response **处于什么状态、归谁**？ | 持久 |
| **ContextStore** | 工单的正文与附件 | input/output 条目 + 物化快照 | 这段对话的**历史是什么**？ | 持久（保留期可配） |
| **ResponseEventLog** | 现场的实时直播流 | 正在产生的增量事件 | 订阅者**此刻看到了哪些增量**？ | 瞬态（终态后按 `retain_ms` 释放） |
| **ConversationStore** | 会话的登记簿 | 链尾指针 + 轮次锁 + 会话事件流 | 这段对话**归到哪个会话、当前轮到谁**？ | 持久（D28） |
| **Agent** | 干活的工人 | 不拥有数据，只驱动流转 | **谁把活干完**？ | 独立进程（`nova-agentd-mock`） |
| **Gateway** | 前台 | — | 请求该不该进 | — |
| **Scheduler** | 外包渠道 | — | 怎么触达模型（含排队限流） | — |
| **ToolExecutor** | 工具间 | — | 模型要调的工具怎么落地 | — |

一句话记住四者：**Ledger 管「状态」，ContextStore 管「历史」，EventLog 管「正在发生的增量」，ConversationStore 管「会话归属与轮次锁」**；Agent 是协调它们的「工人」，自己不留数据。

**一个 response 就是一个 agent 的完整执行**：Agent 内部跑 ReAct 循环——模型要工具就调 `ToolExecutor`，把结果喂回模型再继续，直到模型给出最终答案。工具调用与结果既进快照（下一轮模型能看到完整轨迹），也作为 `output_item.*` 事件流式推送给订阅者。

### 5.2 三阶段时序

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
    GW-->>C: 202 / SSE / 等终态

    Note over EN: 轮询 claim。网关不通知执行端，<br/>执行与接入解耦
    EN->>LG: claim(agent_id)
    LG-->>EN: ClaimedResponse{record 已含快照, attempt}
    EN->>EV: append(InProgress)
    Note right of EN: 快照随 claim 返回（record.context），<br/>不读 ContextStore
    loop ReAct loop（≤ max_tool_rounds）
        EN->>SC: schedule(request, sink)
        SC->>PR: outbound call
        loop 增量
            PR-->>SC: text chunk
            SC-->>EN: text_delta
            EN->>EV: append(delta, attempt)
        end
        SC-->>EN: CompletionsOutcome{finish}
        alt finish = ToolCalls
            EN->>TE: call(name, args)
            TE-->>EN: output
            EN->>EV: append(output_item.added / done)
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
    GW->>LG: create(record.context = 快照)
    GW->>CX: put(record 含快照)
    GW->>EV: append(Created)
    GW-->>C: 202

    EN->>LG: claim(agent_id)
    Note right of EN: 快照已在创建时固化，随 claim 返回；<br/>不再回溯 previous，祖先缺失不影响本环
    EN->>SC: schedule(含完整历史)
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
    Note right of GW: 若属于某会话，向会话事件流<br/>广播 ResponseDeleted（D28）
    GW-->>C: 200

    C->>GW: 订阅次轮续订
    GW->>CX: resolve_chain(次轮)
    CX-->>GW: 完整历史，含首轮内容
    end
```

###### **这张图要传达的三件事**：

1. **两条写路径从不交汇**：增量走 `ResponseEventLog`（瞬态），输出条目走 `ContextStore`（持久）。`Agent` 是唯一同时触碰两者的组件，但它把「流式给订阅者看」和「终态提交存储」作为两次独立写入（§8）。
2. **快照在创建时固化、执行时读取**：阶段二里 `previous` 的历史在 `create` 那一刻被解析成扁平快照，随后执行只是单次读取——这就是「祖先缺失不影响本环」的由来（D24）。
3. **删除是记录级的，下游存活**：删除首轮只删那条记录，次轮快照里的扁平副本原样保留，次轮仍可解析完整历史，而非像旧链式方案那样整链断裂。

---

## 6. 创建时序（三种投递模式）

```mermaid
sequenceDiagram
    participant C as 调用方
    participant R as routes（接入层）
    participant V as service（能力层）
    participant L as ResponseLedger
    participant S as ContextStore
    participant E as ResponseEventLog

    C->>R: POST /v1/responses

    rect rgba(92, 26, 26, 0.25)
    Note over R: 接入层准入检查（顺序有意义）
    R->>R: 1 租户鉴权
    R->>R: 2 draining？→ 503
    R->>R: 3 preflight 定向补救说明
    R->>R: 4 严格解析（未知字段→400）
    R->>R: 5 validate（规模 / URL / 条目类型）
    end

    R->>V: create(tenant, request, input_items, previous, key)

    rect rgba(26, 77, 92, 0.3)
    Note over V,S: 7 先解析前驱（或会话锚点），后创建
    V->>S: resolve_chain(tenant, previous, limits)
    S-->>V: 扁平快照 / ChainBroken·TooLong·NotStored
    Note right of V: 断裂检查必须在创建前失败，<br/>否则留下永不可用的半创建记录
    end

    opt 有 conversation
    V->>V: 8 acquire_turn 取轮次互斥标记
    Note right of V: 已有轮次在途即 Busy（409）；<br/>持有者已终态则接管残留标记（D28）
    end

    V->>L: 9 create(record, idempotency_key)
    alt Accepted
        V->>S: 10 put(record)（store=true 时）
        V->>E: 11 append(Created, seq=0)
        Note right of E: 立即订阅者也能看到确定起点
        V-->>R: Accepted{record}
    else Duplicate
        V-->>R: Duplicate{原生成}
        R-->>C: 返回原生成（绝不产生第二个）
    else ReadOnly / Overloaded
        V-->>R: 降级 / 过载
        R-->>C: 503 / 429
    end

    alt stream=true
        R-->>C: 12a SSE 流（同连接）
    else background=true
        R-->>C: 12b 202 + 生成对象
    else 默认（同步）
        R->>V: 12c wait_terminal（超时返回当前状态供轮询）
        R-->>C: 生成对象
    end
```

**核对点**（`routes/responses.rs::create` 与 `service/responses.rs::ResponsesService::create`）：

- **分层边界**：协议解析、鉴权、路由、HTTP 状态映射在 `routes`；`resolve_chain → acquire_turn（会话锁）→ create → put → append(Created)` 这条业务主线在 `service`，其中**不出现** axum 类型。故 responses 业务逻辑可用纯异步测试覆盖，无需启动 HTTP 服务
- **步骤 7 在 9 之前**是刻意的：链断裂若发生在创建之后，会留下一条永不可用的半创建记录
- **会话锁在 service 层取**（D28）：无论走标准 `/v1/responses` 还是任何门面，`TurnStarted` 都不会漏发；`conversation` 只是第二种指定锚点的方式，最终都收敛到 `resolve_chain`
- 幂等重放返回**原生成**，不产生第二个（`CreateResult::Duplicate`）
- `Created` 事件 `seq=0`，是「0 基连续」的起点
- 三模式共用**同一条内部事件流**；同步模式只是服务端替调用方等这条流的终态
- 网关创建后即返回，**不通知执行端**：`nova-agentd-mock` 轮询领取，与创建节点无关

---

## 7. 执行时序（独立执行 + attempt 栅栏）

```mermaid
sequenceDiagram
    participant E as Agent
    participant L as Ledger
    participant S as ContextStore
    participant B as 在途缓冲
    participant T as ToolExecutor
    participant P as provider

    Note over E: 轮询领取。claim 频率 ≈ 账本写频率，<br/>与增量差三个数量级，不构成压力
    E->>L: claim(agent_id, now_ms, exec_ttl_ms)
    Note over L: 单点原子转换，attempt 递增<br/><b>全局领取任意 queued</b>
    L-->>E: ClaimedResponse{record, attempt}
    E->>B: append(InProgress, attempt)

    Note right of E: 快照随 claim 返回（record.context）；<br/>历史已在创建时物化，零次额外读，<br/>不再回溯，祖先缺失不影响执行
    Note over E: spawn_heartbeat：后台保活，<br/>防止长循环被 sweeper 误 reap

    loop ReAct loop（≤ max_tool_rounds）
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
            E->>B: append(output_item.added / done)
        else finish = Stop / Refusal
            Note over E: 完成
        else finish = Length
            Note over E: 截断 → Incomplete
        end
    end

    E->>L: complete(attempt, status, usage)
    opt record.stored
    E->>S: <b>append_output(call + output + answer)</b>
    Note right of S: 第二条独立写入路径<br/>非事件流回放
    end
    Note over E: settle：推进会话尾（advance）<br/>再释放轮次标记（release_active，D28）
    E->>B: append(终态事件, attempt=None)
    E->>B: close(retain_ms)
```

**核对点**（`crates/agent/src/engine.rs`）：

- **`claim` 无节点参数**：任意执行进程领取任意 queued 生成。`check-deps` 会拒绝 `claim` 重新带上 `NodeTag`
- 上下文由**创建时固化**的快照提供：快照随 `claim` 返回（`record.context`），Agent 零次额外读取；scheduler 无租户上下文，不得自行解析历史
- 栅栏：`append` 携带 attempt，被取代的持有者写入返回 `StaleAttempt` → `SinkVerdict::Stop`（见 `LedgerSink`）
- **终态事件 `attempt: None`**：栅栏已由 ledger 转换校验过，此处再校验会拒掉宣告转换的那条事件，流将永不终止
- `Executed` 的四个取值（`Idle`/`Completed`/`Superseded`/`Failed`）刻意区分，测试可断言走过哪条路径而非只看最终状态。特别地 **`Superseded` 不是失败**：活已归属新 attempt，报失败会终结一个正在被服务的响应
- 并发上限约束**本进程**（`agentd` 的 `--max-concurrent`）；provider 侧限流属 scheduler

### 7.1 为何 attempt 栅栏不可删除

全局 claim 是单点原子转换，并发双领在结构上不可能。但栅栏仍不可删：执行任务卡死超过执行上界会被 sweeper 回收（抬高 attempt）；若该任务此后苏醒并追加，其 attempt 已过期，必须被拒（FR-6 / CR-7 / INV-6）。

> 「claim 已原子，故可去掉 attempt」是错误推论：**卡死任务的苏醒写入与回收是并发的，与领取无关**。

---

## 8. 两条写路径（最关键的一张）

```mermaid
graph TB
    X["Agent 产出"]

    subgraph P1["路径 1：增量事件"]
        E1["EventLog.append"]
        E2["在途缓冲<br/><b>有界 · 瞬态</b>"]
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

- 两条路径由 Agent **分别写入**，无派生关系：`engine.rs` 的 `complete` 中 `ledger.complete` / `context.append_output` / `event_log.append` 是三次独立写
- 若输出条目由事件流回放派生，则持久历史将依赖一个随时可被驱逐的有界缓存（INV-48）
- 已由 L0 `output-provenance` 验证：**销毁事件流后，已存输出必须依然完整**（`conformance` 的 `assert_output_provenance`）

**实际后果**：共享缓冲下网关被 SIGKILL 不再丢失在途流（缓冲在 `mem-server` 共享载体，执行在别的进程）。但增量始终是瞬态的——终态后按 `retain_ms` 释放，之后只能读持久历史。这是分层的代价与收益的交换点。

---

## 9. 订阅与续订

```mermaid
sequenceDiagram
    participant C as 调用方
    participant N as 任意节点
    participant B as 在途缓冲

    C->>N: GET /v1/responses/{id}?stream=true&starting_after=N
    Note over N: 存储是共享载体，任意节点直读，<br/>无路由分支

    N->>B: 探测 read_after(cursor, limit=1, wait=0)
    Note over N,B: 先探测再开流：状态码一旦随 200 提交，<br/>就只能改用带内 error 事件，<br/>而客户端普遍忽略它

    alt 位点已被驱逐
        B-->>N: Expired
        N-->>C: <b>410</b>
        Note over C: 不返回部分数据<br/>不静默换位点<br/>无恢复路径
    else id 未知 / 跨租户
        B-->>N: Unknown
        N-->>C: 404（二者不可区分，避免 id 枚举）
    else 正常
        N-->>C: 200 + SSE
        loop 长轮询批读
            B-->>N: 事件批次
            N-->>C: seq 连续，游标排他，每帧带 id
        end
    end

    Note over C,B: 见到终态事件 → 流结束
```

**核对点**（`nova-responses/src/sse.rs`）：

- **先探测后开流**（`open_stream` 的首个 `read_after`）：未知 id / 过期位点得到真正的 HTTP 状态码，而不是 200 之后的带内错误
- `starting_after` 是**排他**游标：`starting_after=0` 跳过 seq 0。`resolve_cursor` 让 `Last-Event-ID` 优先于查询串——它反映客户端**实际收到**了什么
- 410 是硬失败（`map_event_log_error` 把 `Expired` 映射为 `GONE`）：不返回部分数据、不换位点、无恢复路径
- 流中途出错时状态码已发出，只能带内报告；但仍显式——不编造部分数据，且流在此结束
- 调用方唯一需持久化的连接级状态是游标 `(response_id, sequence_number)` → 换连接、换设备、换节点都能续订（FR-11 / FR-32）
- **同一套 `SseSource` 骨架服务两种流**（D28）：per-response 事件流（`ResponseSource`，见终态即结束）与会话事件流（`ConversationSource`，永不自终，`is_terminal` 恒 false）共用先探测后开流、游标纪律、keep-alive 与带内错误上报——不同的只是「读什么、事件名、能否自终」

---

## 10. Agent 与出站调度的职责边界

```mermaid
graph LR
    subgraph AG["执行进程"]
        ENG["<b>Agent</b><br/>全局 claim · 本进程并发上限<br/>栅栏 · 失败归类 · ReAct loop"]
    end

    subgraph CORE["core（端口）"]
        PORTS["Ledger · EventLog · Context · Conversation"]
        REQ["<b>CompletionsRequest</b>"]
        SCH["<b>CompletionsRequestScheduler</b><br/>出站边界"]
        TOOL["<b>ToolExecutor</b><br/>工具出站边界"]
    end

    subgraph AD["适配层"]
        M1["EchoScheduler"]
        M2["ScriptedScheduler"]
        M3["<b>HttpChatCompletionsScheduler</b><br/>真实 provider（HTTP）"]
        M4["<b>CalculatorTool</b><br/>ToolExecutor 实现"]
    end

    P(["provider"])

    ENG -->|"claim / complete / append"| PORTS
    ENG --> REQ
    ENG -->|"finish=ToolCalls 时"| TOOL
    REQ --> SCH
    SCH -.-> M1
    SCH -.-> M2
    SCH -.-> M3
    TOOL -.-> M4
    M3 --> P

    style AG fill:#4a3a6b,stroke:#9b7fc7,color:#fff
    style CORE fill:#1a4d5c,stroke:#4db8d4,color:#fff
    style M3 fill:#3a3a3a,stroke:#777,color:#aaa
```

### 10.1 为何 Agent 与 scheduler 不能合并

| 若合并方向 | 后果 |
|---|---|
| 领活循环并入 scheduler 端口 | 每个 provider 适配器都要重新实现领取、栅栏、失败归类 |
| provider 关切放进 Agent | 限流成为**集群形状的属性**，换 provider 就要重新调部署 |

职责切分：

- **Agent 决定**：何时领活、本进程并发上限、失败是否终结响应
- **scheduler 决定**：一切与触达模型有关的事，**包括排队与限流**

### 10.2 命名为 Scheduler 的实际后果

`CompletionsRequestScheduler` 比 `Executor` 更宽——允许实现内部做排队、限流、批处理、连接池复用、跨 provider 重试。出站边界确实需要这些。

由此产生的规矩：**provider 侧并发策略属于端口之后**。`SchedulerError` 因此区分 `Refused`（队列已满，**未发出**，重试不额外花钱）与 `Unavailable`（已发出并耗费）。

---

## 11. 后台维护与优雅停机

```mermaid
graph TB
    subgraph SW["sweeper：单循环三件事（2s 一跳，独立进程 nova-responses-sweep）"]
        T1["1 reap 失联 claim<br/>抬高 attempt 栅栏 · 释放轮次标记<br/>部分用量由 ledger 自身记账"]
        T2["2 释放过期事件缓冲"]
        T3["3 清理过期内容"]
    end

    subgraph DR["SIGTERM → drain"]
        D1["accepting = false"]
        D2["创建 → 503 draining"]
        D3["<b>读与订阅继续服务</b>"]
        D4["在途完成或超预算后退出"]
    end

    style T1 fill:#1a4d5c,stroke:#4db8d4,color:#fff
    style D3 fill:#1a5c2a,stroke:#4dd47a,color:#fff
```

**核对点**（`nova-responses/src/sweeper.rs`、`nova-responses/src/shutdown.rs`、`crates/sweep/src/main.rs`）：

- 三件事合并为**一个**循环：三个独立循环意味着三个定时器和三次忘记其一的机会
- 部分用量在回收时由 ledger 自身记账，故两步之间崩溃不会丢失（INV-51）
- reap 是**失联执行的唯一收口**（INV-45）。执行进程独立后没有「启动时扫自己的孤儿」这一步可依赖——崩掉的 worker 不会再启动，只能由 sweep 进程抬 fence 并置失败；回收也是终态迁移，故同时释放会话轮次标记（D28）
- drain 期间**读与订阅继续服务**（`AppState::accepting`）——这是滚动发布不中断在途流的原因
- **网关 drain 不影响执行**：`nova-agentd-mock` 是独立进程，故障域已分离；sweep 也是独立进程（`nova-responses-sweep`）

---

## 12. 验证分层与架构的对应

| 层 | 驱动方式 | 后端 | 覆盖什么 | 入口 |
|---|---|---|---|---|
| **L0** | 直调端口，16 项契约用例 | mem | 端口语义：事件日志、账本、取消、上下文链、完整性、`global-claim`、输出溯源、持久化顺序、并发、协议子集、会话（conversation）… | `testing/conformance` |
| **L1** | YAML 场景直驱端口 + Trace/Oracle | mem | 领域行为，**刻意绕过 HTTP**，故失败可定位到领域层 | `testing/scenarios/l1` |
| **L2** | 真实多进程 + HTTP | mem（共享载体 `mem-server` + 独立 agentd + 独立 sweep） | 协议契约、幂等、只读、过载、跨节点订阅、上下文链、会话 | `testing/scenarios/l2` |
| **L4** | 官方 Python SDK 驱动 conversation 端点 | mem（经 gateway HTTP） | 上游 SDK 兼容（D27） | `testing/sdk-compat/run.py` |

- L0–L2 **不得需要数据库**（D17）；L4 无 python3/openai 时是**跳过而非失败**
- gateway 另有 HTTP 契约测试（`crates/nova-responses/tests/http_contract.rs`），在进程内驱动 Agent 走完 claim → ReAct → complete

### 12.1 check-deps 守的是哪些结构性事实

`just check-deps`（`xtask`）把几条无法用单测表达的结构约束变成门禁：

| 门禁 | 若失效会怎样 |
|---|---|
| `core` 无 workspace 内依赖 | 领域层被适配器污染，分层失去意义 |
| `nova-agent-runtime` 无 `reqwest`/`hyper`/`axum`/`sqlx` | 工作循环不再能脱离 socket 测试 |
| `adapters-completions-mock` 无 HTTP/DB/`nova-agent-runtime` | 某个测试可能悄悄发出真实调用 |
| 执行不经 HTTP 端点领活 | 执行走 HTTP 拉取协议，多一跳、多一处鉴权、多一处栅栏校验 |
| `claim` 不带 `NodeTag` | 队列中的生成被搁死在没有执行端的节点上 |
| 协议子集文档与代码一致 | 已发布子集与实现漂移 |

> 门禁自身也需要能被伪证：`claim` 不得带 `NodeTag` 这条检查匹配的是精确的签名片段 `node: &NodeTag`（见 `check_execution_claims_globally_through_the_port`），而非宽泛的 `node_tag` 字样——后者会被解释该谓词的注释满足。**能被自身文档满足的门禁什么也没检查。**

---

## 13. 仍待确认的判断

| # | 判断 | 现状 |
|---|---|---|
| 1 | **`ResponseLedger` 缺少读取部分用量的方法** | `partial_usage_count` 只在 mem 具体类型上，不在 trait 内（trait 只有写方法 `record_partial_usage`），故只持有端口的计费方取不到已记账金额。CR-11 目前由 L1 经 Trace 覆盖，而非端口契约。若计费确需经端口取数，这是一处真实缺口 |

真实 provider（`adapters-completions-http`）的实现约定：

| 决策 | 约定 |
|---|---|
| `base_url` / `api_key` 来源 | **仅环境变量**（SEC-4：密钥不入配置文件与日志） |
| SSE 增量解析 | 在适配器内；Agent 只见 `text_delta` |
| 超时与重试 | 在**适配器内**（§10.2：并发策略属端口之后） |
| 限流 | 同上。`SchedulerError::Refused` 表示「未发出即拒」 |

---

## 附：权威来源对应表

| 本文档章节 | 权威来源 |
|---|---|
| 1 部署形态 | `crates/gateway/src/main.rs`（`mount`）· `crates/agentd/src/main.rs` · `crates/mem-server/src/main.rs` · `crates/adapters/mem/src/proto.rs` · `crates/adapters/mem-client/src/lib.rs` |
| 2 Crate 分层 | 各 `Cargo.toml` 的 `[dependencies]` |
| 3 协议表面 | `nova-responses/src/routes/mod.rs` · [`06-protocol-subset.md`](./06-protocol-subset.md) |
| 4 无转发 | `nova-responses/src/routes/responses.rs`（retrieve / stream / cancel / delete 委托 `state.service`）· `nova-responses/src/config.rs` |
| 5 端到端总览 | 本文档 §6/§7/§9 的综合 · `decisions.md` D24 |
| 6 创建时序 | [`01-responses-api.md`](./01-responses-api.md) · `nova-responses/src/routes/responses.rs` · `nova-responses/src/service/responses.rs` |
| 7 执行时序 | `crates/agent/src/engine.rs` · `crates/agent/src/lib.rs` 模块注释 |
| 8 两条写路径 | [`invariants.md`](../architecture/invariants.md) INV-48 · [`03-context-chain.md`](./03-context-chain.md) |
| 9 订阅续订 | `nova-responses/src/sse.rs` |
| 10 职责边界 | `core/src/ports/completions.rs` · `core/src/ports/conversation.rs` · `decisions.md` D25 · D28 |
| 11 维护与停机 | [`05-reliability.md`](./05-reliability.md) · `nova-responses/src/sweeper.rs` · `crates/sweep/src/main.rs` |
| 12 验证分层 | [`02-verification.md`](./02-verification.md) · `xtask/src/main.rs` |
| 决策推导（本文档不重复） | [`decisions.md`](../architecture/decisions.md) |
| 需求编号 | [`spec.md`](../requirements/spec.md) |

### 已知文档不一致

`docs/design/01-session-stream.md` 描述的是已被取代的 Session / Turn 方案（在 `design/README.md` 中标注为「历史」），不描述当前系统。

`design/README.md` 的「正式设计」表只列了 01-session-stream（历史）与 02-verification，而 `01-responses-api.md`、`03-context-chain.md`、`04-content-integrity.md`、`05-reliability.md`、`06-protocol-subset.md` 均未列入——**索引不全，待补**。
