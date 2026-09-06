---
name: openai-conversations-compat
overview: 以「conversation 退化为链尾指针」大幅简化设计：只实现官方 4 个 conversation CRUD 端点与 responses 的 conversation 参数，上下文唯一路径仍是既有 resolve_chain（D24 零改动）；自研 session 层负责事件流、锁与业务事件，另加 transcript 流式端点供页面恢复；两个新端口均具备 mem/mem-client/sql 三实现并以同一套 L0 断言验证，新增自跳过的 L4 官方 Python SDK 兼容层，15 个关键场景时序图全部落盘设计文档。
todos:
  - id: core-domain
    content: 在 crates/core 新增 SessionId 与 ConversationId、session 与 conversation 领域类型、两个新端口 trait
    status: completed
  - id: core-protocol
    content: 解除 conversation 拒绝条目，新增两组 DTO 与 conversation 参数三形态解析及互斥校验
    status: completed
    dependencies:
      - core-domain
  - id: adapters
    content: 实现 mem、mem-client、sql 三套两个新端口，含 proto wire 变体、migration 与复合索引
    status: completed
    dependencies:
      - core-domain
  - id: service-routes
    content: 新增 sessions 与 conversations 能力层与接入层，泛化 sse 骨架，接通指针化上下文与 gateway 装配
    status: completed
    dependencies:
      - core-protocol
      - adapters
  - id: engine-fanout
    content: 用 [subagent:code-explorer] 审计构造点与全部终态路径，在引擎终态处 end_turn 与 advance
    status: completed
    dependencies:
      - service-routes
  - id: verify-l0
    content: 扩展 conformance 的 PortSet 与 cases 表，补锁 CAS、并发无重号、last-write-wins 等断言与 REST 契约测试
    status: completed
    dependencies:
      - engine-fanout
  - id: verify-l4
    content: 新增 testing/sdk-compat 官方 Python SDK 兼容层与 xtask 自跳过 l4 层及依赖门禁
    status: completed
    dependencies:
      - verify-l0
  - id: docs
    content: 产出 ADR D26 与 D27、两份编号设计文档（含 18 张时序图）与 realtime 草稿，更新子集契约与不变量
    status: completed
    dependencies:
      - verify-l4
---

## 用户需求

现有服务只提供「单次生成」（Responses）能力，需扩展为完整会话能力，满足三条业务诉求：

1. **多端跨设备同步**：同一账号两台设备打开同一会话页面，A 发起流式对话时，B 即使未操作页面也能看到相同的逐字回复
2. **业务事件排入对话时序**：业务侧操作事件与对话事件保持相对时序；重开页面时业务事件也能被拉取并恢复界面
3. **会话级实时订阅**：断线可凭游标续订，无重复无遗漏

**本轮确定的关键简化**：保留极小的官方 conversation 子集——conversation 退化为「指向链尾的指针」，上下文串联完全在 responses 层用既有物化快照完成，会话层不再操心上下文。

**本轮拍板的两个决策**：

- 页面恢复走自研 `transcript` 流式端点，一次吐全量历史，不强制业务分页
- 链尾指针并发更新采用 last-write-wins，不加乐观锁，以免官方 SDK 直连时收到非官方错误

## 产品概述

服务对外呈现两层能力，职责正交。

**兼容层（官方标准）**：会话容器的创建、读取、标签更新、删除四项操作。生成请求携带会话标识即自动继承该会话的全部历史作为上下文，生成完成后自动推进会话链尾。调用方可用官方 SDK 直连。

**会话层（自研，独立命名空间）**：为每个会话提供一条持久、严格有序、可多端订阅的事件流，承载轮次开始与结束、响应删除，以及业务侧自定义事件。任意设备可从任意游标订阅，先补齐历史再转实时推送。另提供对话历史的流式全量拉取，用于重开页面恢复界面。

两层严格分离：对话内容唯一存放处是既有上下文库；事件流只携带引用与业务数据，绝不复制内容。

## 核心功能

**会话生命周期与订阅**

- 创建会话（自动关联一个会话容器）、读取（含当前占用状态）、删除
- 事件流订阅：从指定游标开始，先回放持久历史再转实时推送，单次调用完成，无需额外快照接口
- 同一会话支持多个订阅者独立投递，慢订阅者不阻塞其他订阅者；断线重连凭游标续订，服务端不保存连接状态
- 首事件之前就订阅时返回空并保持连接，不报错

**会话事件类型（最小集）**

- 会话已创建、轮次开始、轮次结束（携带终态）、响应已删除
- 业务自定义事件：携带业务类别与任意业务数据，与对话事件在同一序号空间严格保序
- 业务事件不进入模型上下文

**会话级并发控制**

- 同一会话同时只允许一个进行中轮次，冲突方收到明确冲突响应
- 占用状态通过事件流广播，各端据此禁用或恢复输入
- 所有终态路径（完成、失败、取消、不完整、超时回收）都必须释放占用

**官方会话容器兼容**

- 容器创建、读取、标签更新、删除，对象形状与官方逐字一致
- 生成请求接受会话标识（支持字符串、对象、空值三种形态），自动继承该会话历史，生成完成后推进链尾
- 与「上一次生成标识」两种串联方式并存

**页面恢复**

- 重开页面：流式拉取对话历史得内容，从游标订阅事件流得状态与业务事件，二者合成完整界面
- 若某轮逐字流已过保留窗口，明确报错并降级到历史内容渲染，绝不静默补半份数据

**质量要求**

- 跨租户访问一律表现为「不存在」，不泄露标识是否存在
- 兼容层未知字段一律明确拒绝；自研层信封字段封闭、业务数据有体积与深度上界
- 事件序号严格 0 基连续、并发下无重号
- 上下文来源不可静默降级，断裂与超限一律显式失败

**自动化验证**

- 两个新存储端口均具备内存实现，同一套断言原样验证内存与真实数据库两种后端
- 新增使用官方 Python SDK 打真实 HTTP 的兼容性验证层，缺少运行时依赖的机器上自动跳过而非失败
- 15 个关键场景全部有验证覆盖并有对应时序图落盘文档

## 一、技术栈

沿用现有栈，不引入新框架、不新增依赖类别。

| 层 | 技术 | 复用点 |
| --- | --- | --- |
| 领域与协议 | Rust + serde（封闭枚举 + `deny_unknown_fields`）+ thiserror | `crates/core` |
| 端口抽象 | `async_trait` + `Arc<dyn Trait>` | `crates/core/src/ports/` |
| REST 接入 | axum（Path/Query/State/Json）+ `axum::response::sse` | `crates/nova-responses/src/routes/`、`sse.rs` |
| 适配器 | mem（`MemStore`）· mem-client（RPC 桩）· sql（sqlx + migrations） | `crates/adapters/{mem,mem-client,sql}` |
| L0 契约 | `testing/conformance` 的 `PortSet` + `cases()` 表 | 同一套断言跑 mem / sql |
| L4 新增 | 官方 openai-python SDK（**仅作验证客户端**） | `testing/sdk-compat/` |
| 编排 | xtask + justfile | `verify --level` |


**关键约束**：官方 SDK 只出现在 `testing/`，绝不进入 `crates/` 任何 crate 的依赖。`xtask` 的 `check_deps()` 需新增门禁断言此事。

---

## 二、实现方案

### 2.1 枢纽决策：conversation 退化为「指向链尾的指针」

```
conversations 表：{ id, tenant_id, last_response_id: Option<ResponseId>, metadata, created_at_ms }
```

`POST /v1/responses { conversation: conv_x }` 的处理链：

1. 读 `conv_x.last_response_id`
2. **把它当作 `previous_response_id`**
3. 走**既有** `ContextStore::resolve_chain` → D24 物化快照
4. 终态时 `conversation.advance(conv_x, 本轮 response_id)` 推进指针

**与官方行为等价**：官方语义是「conversation 中的 items 会被前置到 input_items 之前」；而 `resolve_chain(last_response_id)` 返回的正是「该链全部祖先的 items + 它自己的 items」（已核实 `crates/core/src/ports/context.rs` L75-94 的契约注释）。从调用方视角完全一致。

**这是本方案最重要的结构收益**：上下文**只有一条路径**。无需 `ContextSource` 枚举、无需两来源归一、无需 `snapshot_items` 热路径方法。`ContextStore` trait 签名与 `resolve_chain` / `context_depth` 语义**结构上不可能被改动**，D24 的全部现有 L0 断言零风险。

### 2.2 职责正交

|  | 职责 | 命名空间 |
| --- | --- | --- |
| **conversation** | 上下文**从哪来**（链尾指针） | 官方标准，`/v1/conversations` |
| **session** | 状态**怎么广播**（事件流 + 并发锁）+ 页面恢复（transcript） | 自研，`/v1/sessions` |


### 2.3 端点全集

**兼容层 4 个（官方逐字对齐）**

| 操作 | 方法 | 路径 |
| --- | --- | --- |
| Create | POST | `/v1/conversations` |
| Retrieve | GET | `/v1/conversations/{id}` |
| Update metadata | **POST**（非 PATCH/PUT） | `/v1/conversations/{id}` |
| Delete | DELETE | `/v1/conversations/{id}` |


对象形状：`{"id":"conv_...","object":"conversation","created_at":1741900000,"metadata":{...}}`；删除返回 `{"id":"conv_...","object":"conversation.deleted","deleted":true}`。

**本期不实现** items 的 4 个端点（create/list/retrieve/delete items）。随之一并消失的官方细节：items 批量上限 20、`limit` 默认 20 / 上界 100、`order` 默认 desc、`include` 8 枚举、delete item 返回 Conversation 对象、删除容器不级联、输入 33 / 输出 29 变体不对称对 D22 ⑥ 的影响。**待核实项从 2 项减到 1 项**。

官方仍须逐条落实：

1. `conversation` 参数类型是 **string 或 `{id}` 或 null**，不是单一 string
2. metadata 16 对 / key ≤ 64 / value ≤ 512，**直接复用现有 `MAX_METADATA_ENTRIES` / `MAX_METADATA_KEY_BYTES` / `MAX_METADATA_VALUE_BYTES`，不新定义**
3. 官方**没有**「列举 conversations」端点，本期同样不提供
4. `StoredResponse::to_response_value()` 须回显 `conversation` 字段（官方 Response 对象含此字段）；`session_id` 属内部记账，**绝不出站**

**自研层 5 个**

| 操作 | 方法 | 路径 |
| --- | --- | --- |
| 创建会话 | POST | `/v1/sessions` |
| 读取会话（含 `lock_state`） | GET | `/v1/sessions/{id}` |
| 删除会话 | DELETE | `/v1/sessions/{id}` |
| 事件流订阅 | GET | `/v1/sessions/{id}/events` |
| 追加业务事件 | POST | `/v1/sessions/{id}/events` |
| 对话历史流式拉取 | GET | `/v1/sessions/{id}/transcript` |


**不新增** `POST /v1/sessions/{id}/turns` 之类自研写端点——写路径完全复用官方 `POST /v1/responses`。

### 2.4 高频与低频分成两条流

|  | session 事件流（自研，新增） | response SSE（现有，**零改动**） |
| --- | --- | --- |
| 内容 | 信封事件：轮次边界、业务事件、响应删除 | token delta 等高频增量 |
| 频率 | 每轮 2-3 条 | 每轮数千条 |
| 命名空间 | per-session seq | per-response seq |
| 持久性 | **持久** | 在途瞬态，终态后短保留窗口 |
| 承载 | 与上下文库同量级，**同库同事务** | 现有在途缓冲（mem / Redis） |
| 多端 | 需新增扇出订阅 | **已支持** |


**已确认的既有能力**：`crates/nova-responses/src/sse.rs` 的 `open_stream` 采用 probe-then-stream + `starting_after` 排他游标，`resolve_cursor` 让 `Last-Event-ID` 优先于查询参数。故**同一个 response 本来就可被多端共订**，逐字流的多端同步无需新建任何东西。缺的只是「B 如何发现有新轮次开始」——这是 session 事件流唯一要补的能力。

由此得两个代价修正：

- **D20 ④「不持久化完整事件历史」无需推翻**：④ 针对 token 级完整增量流；session 信封事件频率低三个数量级，不在其列。ADR 中澄清边界即可。
- **D21 三类存储无需新增第四类**：session 事件与 ledger/context 同库同事务，复用 `MemStore` 与同一 `PgPool`。

### 2.5 会话事件类型最小集

```rust
pub enum SessionEventKind {
    SessionCreated,
    TurnStarted     { response_id: ResponseId },
    TurnCompleted   { response_id: ResponseId, status: ResponseStatus },
    ResponseDeleted { response_id: ResponseId },
    Business        { kind: String, payload: serde_json::Value },
}
```

- **铁律**：对话内容一律不进 events，只放引用（`response_id`）。内容唯一真相源是 `ContextStore` 的物化快照。
- **不设** `LockAcquired` / `LockReleased`：与 `TurnStarted` / `TurnCompleted` 表达同一事实，属重复。锁状态由「TurnStarted 未配对 TurnCompleted」隐含表达，且 `GET /v1/sessions/{id}` 的 `lock_state` 可一次读到。此简化须在 ADR 记录理由。
- **不设** `ItemsCommitted` / `ItemsRemoved`：无 `ConversationItemId` 概念。
- `Business` 是唯一开放变体，且**不进入模型上下文**（上下文装配只走 `resolve_chain`，从不读事件流）。
- `ResponseDeleted` 的发射点已核实为 `crates/nova-responses/src/service/responses.rs` L276-286 的 `ResponsesService::delete`（**已在 service 层**，符合「编排在能力层」分层，无需下移）。

与 D22 ②「严格拒绝未知」的关系须在 ADR 立**分层规则**：兼容层保持封闭枚举 + `deny_unknown_fields`；自研层信封字段封闭、`payload` 内部不校验但有体积上界与 `json_depth` 上界。

### 2.6 锁与事件必须同一端口（原子性驱动）

`SessionStore` 单端口承载元数据 CRUD + 锁 CAS + 事件流，**不拆两个端口**：锁 CAS 与 `TurnStarted` 事件必须原子，拆开则崩溃窗口内会出现「已上锁但流上无事件」或反之，各端 UI 与服务端状态永久分歧。

```rust
#[async_trait]
pub trait SessionStore: Send + Sync {
    async fn create(&self, session: Session, now_ms: u64) -> Result<Session, SessionError>;
    async fn get(&self, tenant: &TenantId, id: &SessionId)
        -> Result<Option<Session>, SessionError>;
    async fn delete(&self, tenant: &TenantId, id: &SessionId) -> Result<bool, SessionError>;
    async fn delete_by_tenant(&self, tenant: &TenantId) -> Result<u64, SessionError>;

    /// CAS idle→busy，成功则在同一事务内追加 TurnStarted。
    /// 已占用返回 SessionError::Busy（接入层映射 409），且不写任何事件。
    async fn begin_turn(&self, tenant: &TenantId, id: &SessionId,
        response_id: &ResponseId, now_ms: u64) -> Result<u64, SessionError>;

    /// CAS busy→idle 并原子追加 TurnCompleted。幂等：重复调用不重复追加。
    async fn end_turn(&self, tenant: &TenantId, id: &SessionId, response_id: &ResponseId,
        status: ResponseStatus, now_ms: u64) -> Result<u64, SessionError>;

    async fn append_event(&self, tenant: &TenantId, id: &SessionId,
        kind: SessionEventKind, now_ms: u64) -> Result<u64, SessionError>;

    /// 读 starting_after 之后的事件（排他游标，与 ResponseEventLog 同语义，INV-11）。
    /// wait_ms 支持长轮询，使持久回放与 live tail 在一次调用内合一。
    async fn read_after(&self, tenant: &TenantId, id: &SessionId, starting_after: Option<u64>,
        limit: usize, wait_ms: u64) -> Result<Vec<SessionEvent>, SessionError>;

    async fn health(&self) -> Result<(), SessionError>;
}
```

游标语义**沿用项目内的 `starting_after`（排他）**，与 INV-11 及 `assert_event_log_conformance` 既有断言一致；**不采用**参考项目 moray 的 `from_seq`（包含）语义，项目内一致性优先。

`ConversationStore` 因指针化而极小，且**无热路径方法**：

```rust
#[async_trait]
pub trait ConversationStore: Send + Sync {
    async fn create(&self, conv: Conversation, now_ms: u64)
        -> Result<Conversation, ConversationError>;
    async fn get(&self, tenant: &TenantId, id: &ConversationId)
        -> Result<Option<Conversation>, ConversationError>;
    async fn update_metadata(&self, tenant: &TenantId, id: &ConversationId,
        metadata: BTreeMap<String, String>) -> Result<Conversation, ConversationError>;
    async fn delete(&self, tenant: &TenantId, id: &ConversationId)
        -> Result<bool, ConversationError>;
    /// 终态时推进链尾。**last-write-wins，无 CAS**（见 2.8）。
    async fn advance(&self, tenant: &TenantId, id: &ConversationId, last: &ResponseId)
        -> Result<(), ConversationError>;
    async fn delete_by_tenant(&self, tenant: &TenantId) -> Result<u64, ConversationError>;
    async fn health(&self) -> Result<(), ConversationError>;
}
```

### 2.7 transcript 端点：已核实的关键约束

**已核实事实**（`crates/core/src/context.rs` L216-222）：`ResolvedContext { items: Vec<ResponseItem>, depth, bytes }` —— items 是**扁平且无来源标签**的。L136-139 注释逐字说明这是刻意设计：「It is flat on purpose: there is no source tag, because nothing ever needs to strip a single ancestor out again」。

**由此定下的处置**：

- transcript 内部链路：`session.conversation_id` → `conversation.last_response_id` → `ContextStore::resolve_chain` → 扁平 items → SSE 逐条吐出，服务端内部分批
- **不在信封里补 `response_id`**：来源标签在 D24 下不可恢复，硬补会需要按 `previous_response_id` 逐跳 `get()`（N+1 读，且祖先可能已被 record-level 删除，结果不稳定）
- **气泡渲染靠 `ResponseItem` 自带的 `role`**，这已足够；「按轮次分组」的能力**刻意不提供**，须在设计文档明确记录为设计选择而非缺陷
- 信封只带 `index`（0 基序），供客户端断言完整性
- 复用 `sse.rs` 抽出的共用骨架，不重复实现 probe-then-stream / keep-alive

### 2.8 last-write-wins（本轮拍板决策 2）

`advance()` **不加 CAS**。理由：加 CAS 会让官方 SDK 裸用 `conversation` 时收到非官方的 409。经 session 的路径由 session 锁（同时只允许一个进行中轮次）防住并发，故实际业务路径不受影响。

**此语义必须以 L0 断言正向固化**（并发两次 advance 后指针等于其中之一且记录不损坏），避免将来被误当 bug 修成 CAS。设计文档须写明这是决策而非疏漏。

---

## 三、实施注记（防回归）

**安全**

- SEC-2：session 与 conversations 全部端点，跨租户与不存在一律 **404 不是 403**；id 解析失败同样按 404（复刻 `routes/responses.rs` 的 `parse_id` 既有做法）
- SEC-5：`SessionId` / `ConversationId` **不嵌 node_tag**（无在途缓冲与定向路由需求，同时避免路由伪造面）
- SEC-7 / INV-52：Business payload 须有体积上界 + `json_depth` 上界；events 读取 `limit` 有硬上界
- INV-50 / D22 ②③：兼容层 DTO 全部 `deny_unknown_fields` + 封闭枚举，禁用 `flatten` 兜底
- 从 `EXPLICITLY_UNSUPPORTED_FIELDS`（`crates/core/src/protocol/mod.rs` L48-52）**只移除 `conversation` 条目**，保留 `context_management` 与 `prompt`；否则新功能会被自己的 `preflight_unsupported()`（`request.rs` L344-354）拦掉
- SQL 全部值绑定禁止拼接；游标分页走复合索引，禁止 OFFSET 深翻页
- Business payload 只用 `serde_json::Value` 安全路径，不做任意类型还原

**可靠性**

- INV-46 / D21 ④：会话库不可用时**拒写 503**，写前沿用 `health()` 探活，绝不降级为静默不存
- `begin_turn` / `end_turn` 必须单事务，锁状态与事件追加原子
- `end_turn` 幂等（引擎重试路径可能重入）；`begin_turn` 拒绝路径**不写任何事件、不留半状态**
- INV-40：在途流过期须显式 410（经现有 `map_event_log_error`），禁止静默从最早可读处续发
- **锁必须在所有终态路径释放**（完成、失败、取消、不完整、reap 回收），须有 L0 断言，否则会话永久锁死

**性能**

- `advance` 与 `begin_turn` 是每轮各一次的写，单行更新，无热路径查询（指针化的直接收益）
- `read_after` 用长轮询（复用现有 `READ_WAIT_MS` 500 量级），避免接入层紧轮询
- 事件 seq 分配必须并发无重号，L0 须在竞争下断言 0 基连续（参照 `testing/conformance/src/lib.rs` L1419-1422 的 `RACERS` 密集序号断言与 `the_concurrency_check_rejects_a_racey_sequence_allocator` 的反向验证思路）
- migration 建 `(session_id, seq)` 复合索引与租户过滤索引；`session_events` 对 `sessions` 建 FK

**日志**

- 沿用现有 tracing 与既有严重度惯例；只记标识与错误，**不打条目正文与 Business payload**（避免 PII 落日志），不 dump 大 payload

**回归控制**

- `ContextStore` trait 签名、`resolve_chain` / `context_depth` / `delete` 语义**零改动**；只重新措辞 `ports/context.rs` L46-52 的命名边界注释，说明新概念为何另开端口
- 领域类型不含展示元素；`object` 固定值与兜底文案只在 routes 渲染层产生
- `sse.rs` 抽出以 reader 闭包参数化的共用流式骨架，response / session events / transcript 三入口薄封装，**不重复实现** probe-then-stream 与 keep-alive
- `StoredResponse` 新增两字段会波及 sql `row.rs` 映射、mem store、conformance `record()` 助手、`crates/core/src/context.rs` L232 的测试 `record()`、agent 构造点，一律 struct-update 语法，须用 code-explorer 全量审计
- `just release`（`--no-default-features --features sql`）必须仍排除 mem 与验证代码，L4 的 Python 资产不得进入发布产物

---

## 四、架构与目录

### 4.1 分层落位（沿用 xtask 门禁强制的既有分层）

```
crates/core            加 SessionStore/ConversationStore 端口 · 领域类型 · DTO · 两个新 Id
    ↑
crates/nova-responses  加 service/{sessions,conversations}.rs（能力层，无 axum）
                       加 routes/{sessions,conversations}.rs（接入层，仅传输）
                       改 sse.rs 泛化共用骨架
    ↑
crates/gateway         Ports 增两字段，mem/sql 两 feature 各装配一处
    ↕ 同层 peer
crates/agent           终态处 end_turn + advance（唯一扇出点）
    ↑
crates/adapters/{mem, mem-client, sql}  两端口三套实现 · proto wire · migration
```

### 4.2 目录结构

```
nova-agent/
├── crates/core/src/
│   ├── ids.rs                      # [MODIFY] 新增 SessionId(sess_ 前缀)、ConversationId(conv_ 前缀)；沿用现有 newtype 全套（parse/Display/FromStr/Serialize/Deserialize + 严格字符集）；IdError 增变体。两者均不嵌 node_tag（SEC-5）
│   ├── session.rs                  # [NEW] Session{id,tenant_id,conversation_id,lock_state,created_at_ms}、SessionEvent{session_id,seq,kind,ts_ms}、SessionEventKind（见 2.5，5 变体）、LockState{Idle,Busy{response_id}}。纯领域，无展示元素、无 object 字段
│   ├── conversation.rs             # [NEW] Conversation{id,tenant_id,last_response_id:Option<ResponseId>,metadata:BTreeMap,created_at_ms}。无 items、无 ConversationItem、无分页类型（指针化后不需要）
│   ├── context.rs                  # [MODIFY] StoredResponse 增 conversation_id/session_id 两个 Option 字段（均 skip_serializing_if）；to_response_value() 回显 conversation 字段、session_id 绝不出站；L232 测试 record() 用 struct-update 补字段
│   ├── ports/
│   │   ├── session.rs              # [NEW] SessionStore trait（见 2.6）+ SessionError（NotFound/Busy/Unavailable/ReadOnly/CapacityExceeded/Internal，与 ContextError 同风格且同样 SEC-2 不区分不存在与跨租户）
│   │   ├── conversation.rs         # [NEW] ConversationStore trait（见 2.6，7 方法无热路径）+ ConversationError
│   │   ├── context.rs              # [MODIFY] 仅重写 L46-52 命名边界注释，说明 conversation 为何另开端口而非扩展本端口；trait 签名与全部语义零改动
│   │   └── mod.rs                  # [MODIFY] 导出两个新端口
│   ├── protocol/
│   │   ├── conversation.rs         # [NEW] CreateConversationRequest{metadata}、UpdateConversationRequest{metadata}；deny_unknown_fields；metadata 复用 MAX_METADATA_* 常量校验
│   │   ├── session.rs              # [NEW] CreateSessionRequest{conversation?}、AppendBusinessEventRequest{kind,payload}、SessionEventsQuery{starting_after}。信封字段封闭；payload 只做体积与 json_depth 校验
│   │   ├── mod.rs                  # [MODIFY] 从 EXPLICITLY_UNSUPPORTED_FIELDS 只移除 conversation 条目；导出新 DTO；按 openapi.yaml 核实互斥性后可能前移 UPSTREAM_SPEC_REVISION（属显式变更须记 ADR）
│   │   └── request.rs              # [MODIFY] CreateResponseRequest 增 conversation 字段（string | {id} | null 的 untagged 枚举，附归一为 ConversationId 的方法）；validate() 增与 previous_response_id 的互斥校验（待核实官方后定是否硬互斥）；新增 RequestViolation 变体
│   └── lib.rs                      # [MODIFY] 门面导出新类型与端口
├── crates/adapters/mem/src/
│   ├── session.rs                  # [NEW] MemSessionStore：复用 Arc<MemStore>；seq 单调分配；begin_turn/end_turn 在同一锁临界区内完成 CAS + 事件追加（原子）；read_after 支持 wait_ms 长轮询（Notify 唤醒）；租户校验
│   ├── conversation.rs             # [NEW] MemConversationStore：复用 Arc<MemStore>；advance 直接覆写（last-write-wins）；租户校验；容量上界
│   ├── store.rs                    # [MODIFY] MemStore 增 sessions / session_events / conversations 三张表，与 ledger/context 共享同一锁以保证跨端口原子（D21 ①）
│   ├── proto.rs                    # [MODIFY] Request/Response 各增 Session/Conversation 变体；ProtoError 增对应变体
│   ├── server.rs                   # [MODIFY] 新变体 dispatch 分支，形态与既有 Context 分支一致
│   └── lib.rs                      # [MODIFY] MemWorld 增 session/conversation 字段并在 new()/with_integrity() 装配
├── crates/adapters/mem-client/src/
│   ├── session.rs                  # [NEW] SessionStore 的 RPC 客户端桩，形态复刻既有 context.rs
│   ├── conversation.rs             # [NEW] ConversationStore 的 RPC 客户端桩
│   └── lib.rs                      # [MODIFY] MemClientWorld 增两字段并共享 read_only 控制
├── crates/adapters/sql/
│   ├── migrations/                 # [MODIFY] 新增 sessions / session_events / conversations 建表；(session_id,seq) 复合索引 + 租户索引；session_events→sessions FK；conversations.last_response_id 允许 NULL 且不设 FK（record-level 删除后指针可悬空，须容忍）
│   └── src/
│       ├── session.rs              # [NEW] SqlSessionStore：复用同一 pool；begin_turn/end_turn 单事务（条件 UPDATE ... RETURNING 或 SELECT FOR UPDATE）；read_after 游标走索引；长轮询用短睡眠重试
│       ├── conversation.rs         # [NEW] SqlConversationStore：全参数绑定；advance 为无条件 UPDATE（last-write-wins）
│       ├── row.rs                  # [MODIFY] StoredResponse 两个新字段的列映射（漏此处会导致会话串联静默失效）；session/conversation 行映射
│       └── lib.rs                  # [MODIFY] SqlWorld 增 session/conversation 字段
├── crates/nova-responses/src/
│   ├── service/
│   │   ├── sessions.rs             # [NEW] SessionsService：会话 CRUD（create 时同建 conversation 并发 SessionCreated 事件）、业务事件追加、events 读取编排、transcript 编排（conversation.last_response_id → resolve_chain）。无 axum 类型。写前 health() 探活
│   │   ├── conversations.rs        # [NEW] ConversationsService：4 个用例编排。写前 health() 探活
│   │   ├── responses.rs            # [MODIFY] create() 接受已归一的 conversation 参数：读 last_response_id 当作 previous_response_id 走既有 resolve_chain（无新增分支逻辑）；关联 session 时先 begin_turn（Busy 由接入层转 409，拒绝路径不写事件）；delete()（L276-286）成功后追加 ResponseDeleted 事件
│   │   └── mod.rs                  # [MODIFY] re-export
│   ├── routes/
│   │   ├── sessions.rs             # [NEW] 6 个 handler（CRUD + GET/POST events + GET transcript）。仅传输：解析、鉴权、游标解析（复用 resolve_cursor 使 Last-Event-ID 优先）、状态翻译；id 解析失败按 404
│   │   ├── conversations.rs        # [NEW] 4 个 handler。仅传输 + object 字段渲染（conversation / conversation.deleted）
│   │   ├── responses.rs            # [MODIFY] 解析 conversation 参数（string|object|null）；Busy 转 409
│   │   └── mod.rs                  # [MODIFY] 挂载新路由（注意 conversations update 是 POST 同路径，与 GET/DELETE 共用 route 链；sessions events 的 GET/POST 亦同路径）
│   ├── sse.rs                      # [MODIFY] 抽出以 reader 闭包参数化的共用流式骨架（probe-then-stream + keep-alive + 错误带内上报）；新增 open_session_event_stream 与 open_transcript_stream 两个薄封装；不重复实现
│   ├── error.rs                    # [MODIFY] 新增 map_session_error / map_conversation_error，与 map_context_error 同风格；Busy→409，Unavailable→503，NotFound→404
│   ├── state.rs                    # [MODIFY] AppState 增 session/conversation 端口与两个 service（全 Arc<dyn …>）
│   └── config.rs                   # [MODIFY] 新增 max_business_payload_bytes、session_events_page_max、session_read_wait_ms 等旋钮（RawConfig 保持 deny_unknown_fields）
├── crates/gateway/src/main.rs      # [MODIFY] Ports 结构增 session/conversation 两字段；mem/sql 两个 mount() 各装配一处；启动期对新端口 health 探活
├── crates/agent/src/engine.rs      # [MODIFY] 终态提交处（紧邻 context.append_output）追加：session.end_turn(status) 释放锁、conversation.advance(response_id) 推进指针；deps 增两个 Option<Arc<dyn …>>；仅当 record 携带对应 id 时执行；**所有终态路径**（完成/失败/取消/不完整/reap）均须覆盖
├── testing/
│   ├── conformance/src/lib.rs      # [MODIFY] PortSet 增 session/conversation 两个 Arc<dyn …> 字段并在 mem_ports() 从 MemWorld 装配（保证同一套断言原样跑 mem 与 sql）；新增断言函数：会话 CRUD 与跨租户不可见、事件 seq 0 基连续、starting_after 排他、并发下 seq 无重号（参照 L1419 的 RACERS 密集序号断言）、锁 CAS 互斥且拒绝路径不写事件、end_turn 幂等、所有终态状态均可释放锁、业务事件与轮次事件同序号空间保序、conversation CRUD、advance 的 last-write-wins 正向固化、指针悬空（last_response_id 指向已删记录）时 resolve 显式失败不静默空；在 cases() 表登记新 ContractCase（含 covers/scope/asserts，满足 every_listed_case_is_dispatched 与 covers_claims_are_substantiated_in_the_named_function 两个自检）；record() 助手补两个新字段
│   ├── sdk-compat/                 # [NEW] 官方 Python SDK 兼容层（L4）。requirements.txt 锁定 openai 版本；脚本用官方 SDK 走完整流程（conversations.create → retrieve → update metadata → responses.create 带 conversation 串联多轮 → 校验第二轮继承历史 → delete；并验证未知字段与互斥的拒绝行为）。仅验证资产，绝不被 crates/ 依赖
│   └── scenarios/                  # [MODIFY] 新增会话与多端场景 yaml，沿用既有结构与 covers 标注
├── crates/nova-responses/tests/http_contract.rs  # [MODIFY] 补新端点 REST 契约：状态码、object 字段、未知字段 400、跨租户与不存在均 404、409 冲突、410 过期、多订阅者各自完整投递、transcript 全量与 index 连续、conversation 参数三形态（string/object/null）
├── xtask/src/main.rs               # [MODIFY] verify 增 l4 分支（探测 python3 + import openai，缺失则打印 SKIPPED 并 Ok，复刻 l3 自跳过模式，D17）；coverage 纳入 l4；check_deps 增门禁：crates/ 不得依赖 sdk-compat 或任何 openai SDK
├── justfile                        # [MODIFY] 文件头注释增 l4 说明（含「self-skips, can never fail a machine without deps」措辞）；verify all 追加 just verify l4
└── docs/
    ├── architecture/decisions.md   # [MODIFY] 新增 D26（自研 session 层）与 D27（Conversations 指针化兼容层），见 4.3；更新「现行生效」与「完整索引」两张表
    ├── architecture/invariants.md  # [MODIFY] 新增不变量：上下文唯一路径（只经 resolve_chain）、事件只放引用、per-session seq 0 基连续、锁与事件原子、所有终态必释放锁、业务事件不进模型上下文、advance 为 last-write-wins
    ├── design/07-conversations.md  # [NEW] 兼容层设计：4 端点表、对象形状、conversation 参数三形态、指针化机制与官方语义的等价性论证、last-write-wins 语义、不实现 items 端点的范围声明与理由、错误映射表、**3 张时序图**（创建并串联多轮 / 两种串联方式并存与互斥 / last-write-wins 并发）
    ├── design/08-session-layer.md  # [NEW] 自研层设计：SessionStore 契约、事件类型表、锁语义、订阅与游标语义、transcript 的扁平无来源标签约束与「按轮次分组刻意不提供」的论证、与官方层的引用关系、为何官方协议满足不了（附官方 WebSocket mode 指南自证引文）、**15 张场景时序图全部落此**
    ├── design/drafts/realtime-alignment.md  # [NEW] 后续阶段基线（按 design/README「仅编号文档可实现、drafts 不可直接实现」硬规则，本期不实现故不得占正式编号）：官方 Realtime 事件名逐字速查、两条易错事实、**3 张 Realtime 时序图**
    ├── design/README.md            # [MODIFY] 正式设计表增 07 与 08 行；草稿表增 realtime-alignment 行；路线图增两环
    ├── design/06-protocol-subset.md # [MODIFY] L63-71 移除 conversation 拒绝条目（保留另两条）；新增兼容层子集范围、所依据 spec revision、自研层与兼容层的校验分层规则
    └── requirements/spec.md        # [MODIFY] 补会话、订阅、transcript 的功能需求条目与编号，供 conformance 的 covers 引用
```

### 4.3 时序图落盘清单（用户硬性要求 3）

**`docs/design/08-session-layer.md` — 15 张**

用户原始 8 个：

1. 单设备首轮：SessionCreated → 订阅 → begin_turn → 逐字 → end_turn + advance
2. 单设备多轮：第二轮读 `conversation.last_response_id` → `resolve_chain`
3. 单设备中断恢复：`transcript` 得内容 + `starting_after=last_seq` 得增量，**不需要渲染快照机制**
4. 多设备单轮提前入会：两订阅者独立投递，两端各自订阅同一 response 的逐字流
5. 多设备单轮 B 后入会：B 拉 transcript 直接渲染最终内容，无需回放 token
6. 多设备多轮提前入会：跨轮次锁与事件连续
7. **多设备多轮 B 在途入会（最强验证）**：B 从 `starting_after=null` 订阅得知在途 `resp_1`，再订阅 `GET /v1/responses/resp_1?stream=true`，凭 `Created` 事件通过 `EventBody::Response { response }` 携带**完整 response 对象（含 input）**这一既有行为（`service/responses.rs` L158-164），同时拿到「用户说了什么」与「已产生的全部 delta」，**无需内容副本、无需新增机制**。此依赖须在文档记为**不可回退**
8. 多设备并发冲突：`begin_turn` CAS 失败 409，**拒绝路径不写任何事件、不留半状态**

补充 7 个边界：

9. 在途缓冲已过期降级：`EventLogError::Expired` → 410 Gone，客户端降级拉 transcript；禁止静默续发（INV-40）
10. 业务事件与对话时序交织：TurnStarted(1) → Business{file_uploaded}(2) → Business{approval_required}(3) → TurnCompleted(4) → Business{workflow_advanced}(5)，单一 seq 空间严格保序，重开页面完整拉回
11. 多节点部署 B 连到另一网关：session 事件在共享库故**直连无需转发**；只有高频在途流仍走现有 `route_inflight` 定向代理（不用 307，避免暴露拓扑）
12. 响应删除多端传播：`DELETE /v1/responses/{id}` → `ResponseDeleted` 使 B 移除气泡；D24 record-level 下已发出生成的物化快照不受影响（须写清此差异，避免被误读为不一致）；若删的正是链尾，指针悬空后下一轮显式失败不静默
13. 生成失败或取消：`end_turn` 携带终态；**所有终态路径必释放锁**，须有 L0 断言否则永久锁死
14. 越权访问 session：跨租户一律 404 不是 403
15. 首事件前就订阅：`starting_after` 为空且无事件时返回空并保持连接，不报错

**`docs/design/07-conversations.md` — 3 张**：创建 conversation 并串联多轮生成；`conversation` 与 `previous_response_id` 两种串联方式并存（含互斥性核实结论）；last-write-wins 并发语义。

**`docs/design/drafts/realtime-alignment.md` — 3 张**：会话建立 + 文本轮次；语音轮次（VAD 自动）+ 打断截断；函数调用往返。

已核实的官方 Realtime 事件名（GA 后规格，**须逐字采用禁止凭记忆改写**）——客户端：`session.update`、`conversation.item.create`、`conversation.item.truncate`、`response.create`、`response.cancel`、`input_audio_buffer.append/commit/clear`、`output_audio_buffer.clear`（WebRTC）；服务端：`session.created/updated`、`conversation.item.added/done`、`input_audio_buffer.speech_started/speech_stopped/committed`、`response.created`、`response.output_item.added|created`、`response.content_part.added/done`、`response.output_text.delta/done`、`response.output_audio.delta/done`、`response.output_audio_transcript.delta/done`、`response.function_call_arguments.delta`、`response.output_item.done`、`response.done`、`response.cancelled`、`rate_limits.updated`、`error`。

须在 drafts 显式记录的两条易错事实：① Realtime 的 `conversation` 与 Conversations API 的 `conversation` **不是同一对象**（前者是 session 内项集合、上限 60 分钟；后者是永久持久化 REST 资源）；② 音频字节**只**由 `response.output_audio.delta` 携带，`.done` 与 `response.done` 均不含音频数据。

**图表规范**：统一 mermaid `sequenceDiagram` + `autonumber`；消息文本内避免冒号（mermaid 分隔符）；沿用仓库既有配置惯例。

### 4.4 ADR 变更（严格遵守「已生效决策不删改，新增 SUPERSEDED BY」）

**D26 自研会话层**，部分 SUPERSEDES D20 ⑤⑥。须写明：官方协议族无一满足三条需求（附官方 WebSocket mode 指南「多端同步须自建扇出与事件持久化」的自证引文）；**D20 ④ 保留**并澄清边界（④ 指 token 级完整增量流，session 信封事件低三个数量级不在其列）；**D21 不新增**第四类存储；事件只放引用的单一数据源铁律；锁与事件必须同端口的原子性论证；不设 LockAcquired/LockReleased 的去重理由；校验分层规则（兼容层封闭 / 自研层信封封闭 + payload 有界）；不复活 D18 的 snapshot_seq 与热冷分层；transcript 扁平无来源标签是 D24 的直接后果而非缺陷；场景七依赖 `Created` 事件携带完整 response 对象且此依赖**不可回退**。

**D27 Conversations 指针化兼容层**，部分 SUPERSEDES D20 ①。须写明：conversation 退化为链尾指针后与官方语义的**等价性论证**；正当理由是**协议兼容而非能力增强**；只实现 4 个 CRUD、**不实现 items 4 端点**的范围声明与省下的官方细节清单；不提供官方没有的列举端点；`advance` 为 **last-write-wins（决策非疏漏）**；`ContextStore` 因此结构上零改动，D24 全部语义不动；指针悬空（链尾被 record-level 删除）的显式失败语义；唯一待核实项（`conversation` 与 `previous_response_id` 是否官方硬互斥，权威来源为 `openai/openai-openapi` 的 `openapi.yaml`；现锚定 `UPSTREAM_SPEC_REVISION = "2025-08-07"`，若需前移属显式变更须记入本 ADR）。

## Agent Extensions

### SubAgent

- **code-explorer**
- Purpose: 五处需要跨文件全量审计，避免编译期之外的静默遗漏。① `StoredResponse` 新增 `conversation_id` / `session_id` 后，定位其在 `crates/adapters/{mem,sql}`、`crates/agent`、`testing/conformance`、`crates/nova-responses`、`crates/core/src/context.rs` 测试助手中的**全部构造点与行映射点**（漏 sql `row.rs` 会导致会话串联静默失效）。② 审计四条 wire 装配链：`MemWorld` 字段、gateway `Ports` 与两个 `mount()` 分支、mem 的 `proto` Request/Response 与 `server` dispatch、`MemClientWorld` 字段，确认两个新端口每处都已接通。③ 定位 `crates/agent/src/engine.rs` 中**所有终态路径**（正常完成、失败、取消、不完整、reap 回收），确保每条都调用 `end_turn` 与 `advance`。④ 定位 `sse.rs` 中 `open_stream` / `resolve_cursor` 的全部调用点，确认泛化骨架后无行为漂移。⑤ 定位 `testing/conformance/src/lib.rs` 的 `cases()` 表、dispatch arm、`record()` 助手三处联动位置，确保新 case 满足两个不可绕过的自检测试。
- Expected outcome: 输出带精确文件路径与行号的构造点、装配点、终态路径、调用点清单，作为改动核对表，避免「sql 行映射漏字段导致串联失效」「某条终态路径漏释放锁导致会话永久锁死」这类静默故障。