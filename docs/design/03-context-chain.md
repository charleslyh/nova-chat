# 设计 03 · 上下文链与会话快照

> 依据：FR-15~19, CR-9, INV-41/42/43/46/49 · [D20](../architecture/decisions.md#d20-交付边界收口存储与订阅分离) · [D24](../architecture/decisions.md#d24-上下文物化每个生成保存完整上下文快照) · [D30](../architecture/decisions.md#d30-会话为系统真相源responses-走事件流)

---

## 0. 一句话

**模型上下文与长期对话记录都从「会话主快照」装配**（D30），不再由每个 response 各自物化一份祖先快照（D24 的 O(n²) 被推翻）。response 退化为「短命执行记录」，只持有元数据 + 锚点；对话内容住在 conversation 的持久物化主快照里，每轮以 delta 追加。

---

## 1. 数据模型

两个正交的载体，各答一个问题：

| 载体 | 回答的问题 | 寿命 | 存什么 |
|---|---|---|---|
| **ResponseRecord**（账本） | 这个 response **处于什么状态、归谁、继承自哪**？ | 持久（`store=false` 例外） | 元数据 + `input_items` + 锚点（`SnapshotRef`） |
| **Conversation 主快照** | 这段对话的**完整历史**是什么？ | 持久（冷存储，保留策略可配） | 物化条目列表，每轮 delta 追加 |

两者不再同表同事务——D30 把 INV-34 的原子性从「创建时」迁到了「终态时」（见 §6）。

### 1.1 `ResponseRecord`：账本元数据（不含快照）

```rust
pub struct ResponseRecord {
    pub response_id: ResponseId,
    pub previous_response_id: Option<ResponseId>,   // 回显指针；锚点来源之一
    pub conversation_id: Option<ConversationId>,    // 会话锚点（D28）
    pub tenant_id: TenantId,
    pub model: String,
    pub instructions: Option<String>,               // 回显用，永不进上下文
    pub tools: Vec<Tool>,
    pub tool_choice: Option<ToolChoice>,
    pub input_items: Vec<ResponseItem>,             // 本轮输入（非历史）
    pub reasoning: Option<String>,                  // 渲染用，不进上下文
    pub status: ResponseStatus,
    pub usage: Usage,
    pub created_at_ms: u64,
    pub completed_at_ms: Option<u64>,
    pub stored: bool,                               // false 不可被引用、终态不落快照
    pub expires_at_ms: Option<u64>,
    pub integrity: Option<String>,
    pub integrity_alg: Option<String>,
    pub node_tag: NodeTag,
    pub idempotency_key: Option<IdempotencyKey>,
    pub owner: Option<AgentId>,
    pub attempt: Attempt,
}
```

**没有 `context` / `context_reasoning` / `context_depth` 字段**——那是 D24 每环物化全量快照的遗产，D30 已移除。

### 1.2 `SnapshotRef`：锚点（继承来源）

`ResponseRecord::anchor()` 从自身字段派生，不是第二份状态：

```rust
pub enum SnapshotRef {
    Root,                         // 无历史
    Previous(ResponseId),         // 裸链：continue from a previous response
    Conversation(ConversationId), // 会话锚点
}
```

派生规则：有 `conversation_id` 即 `Conversation`；否则有 `previous_response_id` 即 `Previous`；否则 `Root`。

---

## 2. 快照读取（D30）

### 2.1 为什么会话主快照而非每环物化

D24 为了「删除中间环节后对话仍能继续」选择了每环物化全量快照，代价是 O(n²) 存储。D30 发现「删除后继续」的真正承载者可以是**会话**而非每个 response：

| 方案 | 每轮存储 | 读取 | 删除中间环后对话 |
|---|---|---|---|
| 走链（D20 原方案） | 自身条目 + 指针 | O(链长)，回溯 | 断裂 |
| 每环物化（D24，被 D30 推翻） | 完整历史，扁平副本 | O(1)，读快照 | 继续 |
| **会话主快照 + delta（D30）** | 元数据 + 每轮 delta | O(1)，读会话 | **继续** |

会话主快照是**一条持续增长的条目列表**，每轮终态把本轮条目 delta 追加进去。存储 O(n)（线性追加），读取仍 O(1)（读会话主快照），删除仍记录级——三者兼得，D24 的代价表被消掉。

### 2.2 创建时校验、执行时读取

创建带锚点的 response 时，`resolve_context` 读会话主快照**只为校验上界**（深度/字节），不把快照复制进 record：

| 断裂 | 创建时行为 |
|---|---|
| 锚点缺失或跨租户 | `ChainBroken` / `CrossTenant`（同形，防标识探测 SEC-2） |
| 前驱 `store=false` | `NotStored` |
| 深度/字节超限 | `ChainTooLong` / `ChainTooLarge` |

断裂在**创建前**失败，不留半创建记录。执行端 claim 到锚点后按 `(tenant, conversation)` 再读一次会话主快照重建上下文——每轮一次读（见 §7 / §8 的执行时序）。

### 2.3 裸链（无会话锚点）

`previous_response_id` 续接若指向一个**没有会话归属**的 response（裸链），D30 下没有持久载体：

- `Previous(id)` → 账本反查 `id` 的 `anchor()`：
  - `Conversation(cid)` → 读该会话主快照（正常续接）；
  - `Root` → 从事件流回放重建该裸 response 自身的 input+output（TTL 内）；
  - `Previous(_)` → 更深层的裸链，无持久归属，按 `ChainBroken` 显式失败。

跨会话的 `previous_response_id` 续接（协议要求）因此保留：只要前驱落在某个会话里，就能经账本映射到会话主快照。

---

## 3. 上限与断裂

| 上限 | 默认 | 超限行为 |
|---|---|---|
| 深度 | 50 环 | `ChainTooLong` |
| 条目数 | 1000 | `ChainTooLong` |
| 累计字节 | 1 MiB | `ChainTooLarge` |

**禁止静默截断**（INV-41）。上限在**创建时 `resolve_context` 校验**：超限即拒绝创建，读取时快照已是合规尺寸。1 MiB 上限之所以有效，依赖「图片文件仅接受引用」这一协议约束（见 [06](./06-protocol-subset.md) §3.1）。

---

## 4. `instructions` 不参与快照

已核实上游语义：`instructions` 是插入上下文最前的 system/developer 消息，**不是条目**，在响应对象上独立回显；**与 `previous_response_id` 一起使用时不被继承**。

由此产生硬约束：

- 按生成单独存储，供 `GET` 回显
- **绝不进入会话快照**
- 完整性签名也不覆盖它——它是元数据而非内容

实现上由 `ResponseRecord` 的 `input_items` / `append_turn` 的 `input_items`/`output_items` 参数保证——`instructions` 字段根本不参与装配。

---

## 5. 链亲和路由

**已移除（D25）。** 存储共享化后，锚点（会话或前驱）在任意节点直接解析，不再有「把创建请求导向链所属节点」的链亲和路由。

---

## 6. 保留与删除

| 项 | 默认 | 性质 |
|---|---|---|
| response 事件流保留 | 30 天 | **配置项**（OR-5）；TTL 后检索 404 |
| 会话快照保留 | 冷存储，独立保留策略 | **配置项**（D30 新增载体） |
| 单条删除 | `DELETE /v1/responses/{id}` | **记录级删除**：删账本记录 + 事件流；会话快照里的继承副本原样保留 |
| 租户清除 | `POST /v1/tenants/{t}/purge` | 需管理凭据；分批执行避免长事务 |
| 过期清理 | sweeper 每 2s，单批 ≤ 500 | 事件流走 `event_log.sweep_expired`；会话快照按保留策略 |

### 6.1 记录级删除，而非内容级抹除

删除一环只移除该环的账本记录与事件流，不做任何级联写。会话主快照是在终态时追加的**独立副本**，因此被删环节的消失不影响会话历史——transcript 仍含完整内容，包括被删环节的条目。

这正符合「从对话移除」（而非「合规抹除」）的语义：删的是「这条 response 记录」，不是「这条内容在会话快照里的副本」。

---

## 7. 终态写入的原子性（INV-34 迁移）

D24 时代 `ledger.create` 与 `context.put` 同事务（创建时原子）。D30 把它迁到**终态**：

```
ledger.complete(id, attempt, status, usage)   // 状态 → 终态
  ↕ 同存储同事务（D21 ①）
conversation.append_turn(tenant, conv,       // 本轮 delta 追加进主快照
    response_id, input_items, output_items, reasoning, usage, status, now_ms)
conversation.advance(conv, resp)             // 推进链尾
conversation.release_active(conv, resp, status)  // 释放轮次标记
```

`append_turn` 的数据源是**执行端终态的 `AgentOutcome.items` 直接提交**，**禁止**从事件流回放派生（INV-48）——否则会话快照将依赖一个随时可被驱逐的有界缓存。

---

## 8. 跨租户隔离

三道防线，任一失效都会导致泄露：

1. **查询与删除**：`tenant_id` 进入谓词，跨租户读与不存在同形
2. **快照读取**：`read_snapshot` / `resolve_context` 校验租户（INV-42），跨租户即拒
3. **协议层**：拒绝 `item_reference`（INV-52），因为它能按标识引用任意条目从而绕过第 2 道

第 3 道容易被忽略——它是协议层面的防线，而非存储层面的。
