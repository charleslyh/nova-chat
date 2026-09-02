# 设计 03 · 上下文链

> 依据：FR-15~19, CR-9, INV-41/42/43/46/49 · [D20](../architecture/decisions.md#d20-交付边界收口存储与订阅分离)

---

## 1. 数据模型

账本与上下文库**同表同事务**。这不是省事：分两表需要跨表事务，且部分失败会留下「账本有记录、内容缺失」的状态——这种链会在**某个后续轮次**才断裂，是最难诊断的失败模式。一行则由构造排除该可能。

| 列 | 用途 |
|---|---|
| `response_id` PK | `resp_{node}_{uuid}` |
| `previous_response_id` | 链指针 |
| `tenant_id` | 逐环校验依据 |
| `stored` | `false` 时不含条目、不可被引用 |
| `instructions` | 供查询回显；**走链时不选取此列** |
| `input_items` / `output_items` JSONB | 条目 |
| `usage` / `partial_usage` JSONB | 终态用量 / 按 attempt 记录的作废用量 |
| `integrity` / `integrity_alg` | 防篡改标签 |
| `node_tag` | 孤儿收口范围 |
| `expires_at_ms` | 过期清理 |

索引：`(tenant_id)`、`(previous_response_id)` 部分索引、`(expires_at_ms)` 部分索引、`(node_tag, status)` 部分索引（供孤儿收口，非终态行是极少数，故该索引很小）。

---

## 2. 走链解析

### 2.1 为什么是走链而非全量物化

| 方案 | 每环存储 | 30 天总量 | 单环删除 |
|---|---|---|---|
| **走链** | 仅自身条目 + 指针 | ≈ 135 GB | **有效**——内容只存一份 |
| 全量物化 | 完整历史 | ≈ 740 GB | **无效**——同一段内容在后续每环重复存在 |

决定性理由不是容量，而是**删除权无法履行**：全量物化下删除一环，其内容仍存在于后续每一环中。

代价是读放大：写 `O(1)`、读 `O(链长)`，故必须配深度上限。

### 2.2 内存实现：单次加锁完成整条遍历

```rust
let g = self.store.lock();          // 一次
while let Some(id) = cursor { … }   // 整条遍历都在锁内
```

**每环一次加锁是本设计最易踩的性能坑**：50 深的链会变成 50 次锁往返，落在每个带链请求的关键路径上。

该性质由结构保证而非纪律保证：`resolve_chain` 函数体内**没有 `.await`**，借用检查器因此让守卫存活于整段遍历。若将来有人插入 await 点，代码将无法按原样编译。

### 2.3 SQL 实现：递归单查询

```sql
WITH RECURSIVE chain AS (
    SELECT response_id, previous_response_id, tenant_id, stored,
           input_items, output_items, 1 AS depth
      FROM responses
     WHERE response_id = $1 AND tenant_id = $2
    UNION ALL
    SELECT r.response_id, r.previous_response_id, r.tenant_id, r.stored,
           r.input_items, r.output_items, c.depth + 1
      FROM responses r
      JOIN chain c ON r.response_id = c.previous_response_id
     WHERE c.depth < $3
)
SELECT … FROM chain ORDER BY depth DESC;   -- depth 降序 = 时间正序
```

**这是选 PostgreSQL 系的决定性理由**：把走链从 N 次往返压成一次查询。深度 50 时是 1 跳 vs 50 跳。

两处易错细节：

1. **深度上限、租户、起点全部绑定参数**（`$1/$2/$3`），无字符串拼接（SEC-8）
2. **递归项不过滤 `tenant_id` 与 `stored`**。若在递归里过滤，跨租户或未存储的环会表现为「链自然结束」——即静默截断。改为选出后在应用层逐环判定，才能给出精确错误（INV-42/43）
3. **不选取 `instructions` 列**（INV-49）

### 2.4 起点与中间环的区别对待

| 位置 | 情形 | 错误 | 理由 |
|---|---|---|---|
| 起点 | 缺失 / 跨租户 | `chain_broken` | 起点是调用方提供的值，两者同形以防标识探测（SEC-2） |
| 中间环 | 跨租户 | `cross_tenant` | 数据内部关系异常，精确报错有助排障 |
| 任意环 | `store: false` | `not_stored` | 该环确实属于本租户，只是无内容——告知真实原因 |

两个后端必须一致，这条由 L0 契约 `assert_context_conformance` 强制。

---

## 3. 上限与断裂

| 上限 | 默认 | 超限行为 |
|---|---|---|
| 深度 | 50 环 | `ChainTooLong` |
| 条目数 | 1000 | `ChainTooLong` |
| 累计字节 | 1 MiB | `ChainTooLarge` |

**禁止静默截断**（INV-41）。静默截断的后果是上下文被无声裁掉、输出质量下降且不可复现——比明确失败糟糕得多。

1 MiB 上限之所以有效，依赖「图片文件仅接受引用」这一协议约束（见 [06](./06-protocol-subset.md) §3.1）。若将来放开内联二进制，此上限必须重估。

---

## 4. `instructions` 不参与走链

已核实上游语义：`instructions` 是插入上下文最前的 system/developer 消息，**不是条目**，在响应对象上独立回显；**与 `previous_response_id` 一起使用时不被继承**。

由此产生硬约束：

- 按生成单独存储，供 `GET` 回显
- **绝不进入走链输出**
- 完整性签名也不覆盖它——它是元数据而非内容，让标签依赖一个从不参与拼接的字段没有意义

违反后果：调用方换了系统提示却仍受旧指令影响。这类问题从外部几乎无法归因。

实现上由 `StoredResponse::chain_items()` 保证——该方法只迭代 `input_items` 与 `output_items`，根本不读 `instructions` 字段。

---

## 5. 链亲和路由及其退役条件

**仅在上下文库非共享时需要。** 带 `previous_response_id` 的创建请求被导向该链所属节点，使走链全程本地完成。

```rust
pub fn route_chain_affinity(state: &AppState, previous: &ResponseId) -> Route {
    if state.content_is_shared() {
        return Route::Local;   // 共享库后必须退役
    }
    resolve(state, previous.node_tag())
}
```

**共享库后必须退役**：否则长会话把每一轮都钉死在同一节点，制造热点。这由 `is_shared()` 单一标志自动完成，无需人工改动接入层。

> 这是与 `route_inflight` 的本质区别：后者是永久架构特征（状态在特定进程堆内，无共享端点），前者是临时措施。把两者描述为「同一种定向转发」会让链亲和被当作正式设计固化下来。

---

## 6. 保留与删除

| 项 | 默认 | 性质 |
|---|---|---|
| 内容保留期 | 30 天 | **配置项**（OR-5） |
| 单条删除 | `DELETE /v1/responses/{id}` | 删除后不可再被引用为上一环 |
| 租户清除 | `POST /v1/tenants/{t}/purge` | 需管理凭据；分批执行避免长事务 |
| 过期清理 | sweeper 每 2s，单批 ≤ 500 | 走 `expires_at_ms` 部分索引 |

内存实现用 `BTreeMap<(deadline, id)>` 作过期索引，按 deadline 有序，`take_while` 到第一个未到期项即停——只触碰真正要删的记录，不全表扫描。

**保留期与是否加密均为配置项，不硬编码合规策略**：不同调用方的合规要求不同，把策略写死会迫使每次调整都改代码。

---

## 7. 跨租户隔离

三道防线，任一失效都会导致泄露：

1. **查询与删除**：`tenant_id` 进入 SQL 谓词，跨租户读与不存在同形
2. **走链**：**逐环**校验（INV-42），遇跨租户环立即中断，不跳过继续
3. **协议层**：拒绝 `item_reference`（INV-52），因为它能按标识引用任意条目从而绕过第 2 道

第 3 道容易被忽略——它是协议层面的防线，而非存储层面的。
