# 设计 02 · 验证体系

> 依据：[D15](../architecture/decisions.md#d15-验证分层与虚拟时钟) · [D17](../architecture/decisions.md#d17-本机验证与-docker-部署分离)

---

## 1. 分层与后端对应

| 层 | 内容 | 后端 | Docker |
|---|---|---|---|
| **L0** | 端口契约（15 用例，含领取局部性、并发、过载完整性、输出溯源） | **内存 + SQL 共用同一套断言** | 内存部分无需 |
| **L1** | 场景（进程内，直驱端口） | 内存 | 无 |
| **L2** | 场景（三节点 HTTP） | 内存 + `route_inflight` 转发 | 无 |
| **L3** | 端到端 | SQL | 允许（D17） |

**L0–L2 完全不依赖基础设施**。L3 无数据库时**跳过而非失败**，否则没有基础设施的开发机会被门禁挡住。

---

## 2. L0 的验收方式：同一套契约跑两个后端

这是端口是否真是抽象的判据。`run_suite` 接收 trait 对象集合：

```rust
pub struct PortSet {
    pub ledger: Arc<dyn ResponseLedger>,
    pub event_log: Arc<dyn ResponseEventLog>,
    pub context: Arc<dyn ContextStore>,
    pub integrity: Option<Arc<dyn ContentIntegrity>>,
    pub node_tag: NodeTag,
}
```

内存后端由 `conformance::mem_ports()` 装配；SQL 后端由 L3 runner 装配后调用**同一个** `run_suite`。断言一行不改。

### 2.1 用例清单是单一真相源

```rust
pub fn cases() -> &'static [ContractCase]   // name + covers + scope
async fn run_case(ports, case) -> CaseOutcome
```

两个入口（静默版与逐项报告版）都遍历 `cases()`；`xtask coverage` 也从这里读 `covers`。

> **修复的缺陷**：原先契约以「两份手抄调用序列 + 门禁里第三份硬编码编号表」的形式存在。给一处新增断言、或从所有处删除断言，**都不会产生任何可见变化**。无法察觉自身内容缩水的门禁不是门禁。
>
> 现在 `cases()` 里列了名字但 `run_case` 没有分发分支 → **panic**（否则会被计为已覆盖却从未执行）。由 `every_listed_case_is_dispatched` 固定。

### 2.2 覆盖率由实际运行导出，而非声明

`SuiteReport::covered()` 只统计**真正跑过**的用例的 `covers`。删掉一项断言，覆盖率立即下降。

同理，静态门禁 `check_protocol_spec_is_publishable()` 返回它所支撑的编号（`FR-23`/`FR-27`），由 `coverage` 汇入——门禁自报覆盖，不在别处再写一份。

### 2.3 覆盖声明必须在断言处可追溯

`covers` 是**自我声明**，而覆盖率由它导出。因此需要一个机制让声明可被伪证：

```rust
pub struct ContractCase {
    pub covers:  &'static [&'static str],
    pub asserts: &'static str,   // 支撑该声明的函数名
}
```

`covers_claims_are_substantiated_in_the_named_function` 读取自身源码，要求 `covers` 里每个编号都出现在 `asserts` 所指函数体内（文档注释、行内注释或断言消息均可）。

> **修复的缺陷**：上一轮把覆盖率改为「从运行导出」，但导出的是**跑过的用例的自我声明**。若声明本身错配，覆盖率依然虚假。门禁启用后一次报出 **12 个用例全部存在无根据声明**，其中 5 项是真实语义错配：

| 原声明 | 问题 | 归属修正 |
|---|---|---|
| event-log → **FR-11** | 「无连接粘性」是接入层性质，存储端口无从观测 | → L2 无粘性场景 |
| event-log → **INV-16** | 事件合并与事件日志读写无关 | → event-coalesce |
| event-coalesce → **FR-11** | 合并关乎载荷体积，与游标状态无关 | → L2 |
| ledger → **FR-5** | ledger 用例不做回收 | → orphan-reclaim |
| ledger → **CR-3 / CR-7** | 只做了 `check_attempt` 探测，非真实写入栅栏 | → output-provenance |
| cancel → **FR-20** | 取消与输出条目来源无关 | → output-provenance（新建） |

剩余项属另一类：断言确实做了，只是代码里没写编号。已逐一补注。

### 2.4 可选端口缺失必须显式报告

`integrity` 为 `None` 时，用例状态为 `SKIPPED` 且**不贡献覆盖**。原先是静默跳过，输出上与通过无从区分——即「没有完整性实现」会被读成「完整性已验证」。由 `optional_port_absence_is_reported_not_hidden` 固定。

### 2.5 用例的 scope 区分后端与协议

| scope | 含义 | 数量 |
|---|---|---|
| `Backend` | 打在端口实现上，换后端结果可能不同 | 10 |
| `Protocol` | 打在共享领域/协议代码上，各后端必然相同 | 4 |
| `OptionalPort` | 端口可缺，缺则显式跳过 | 1 |

区分的意义：协议用例在两个后端各跑一次**不等于协议被验证了两次**，那样呈现会高估后端覆盖。

### 2.6 conformance 不依赖 adapters-sql

否则 L0 编译需要数据库驱动，违反 D17。SQL 后端由 L3 runner 注入。此约束由 `just check-deps` 强制。

### 2.7 每个用例使用独立租户

`fresh_tenant()` 生成随机租户，故契约套件可对持久化后端**重复运行而无需清库**。

> 但 `claim` 是从**节点全局队列**取，不按 id 取，因此并发用例断言的是「无响应被领取两次」，而非「只有一个 claim 成功」——后者会被同套件其他用例遗留的排队记录破坏。初版正是写错成后者，产生了一个与正确性无关的失败。

---

## 3. 补上的三项实质缺口

复查「声称覆盖」与「实际断言」的对应关系后，发现三处核心性质从未被验证。

### 3.1 FR-20：输出条目的来源（本次最重要）

FR-20「输出条目由执行端在终态直接提交；**不由事件流回放派生**」是整个重构的支点决策（D20），而它**没有任何验证**——编号挂在 cancel 用例上，而该用例根本不触及它。

新增 `output-provenance` 用例，以其决定性后果验证：**销毁事件流，已存输出必须依然完整**。

```
append 三个 delta → append_output 提交终态条目
→ close + sweep 事件流（read_after 确认返回 Expired）
→ 断言 output_items 仍含完整文本
```

若输出由回放派生，这一步会得到空条目。缺少此验证时，未来修 bug 最省事的做法恰恰是「回放 delta 重建输出」——而事件缓冲是有界瞬态的，那会让持久历史依赖于一个随时可被驱逐的缓存。

同一用例还接管了 CR-3/CR-7/INV-6：用**真实 append 路径**验证失效持有者被拒。原先 ledger 用例只调 `check_attempt` 探测——实现完全可以通过探测、却接受紧随其后的写入。

### 3.2 FR-4：领取必须限定本节点（D23 修复的缺陷）

拉取协议依赖一个从未写明的前提：**领取方即宿主节点**。内存后端各持自有账本，该前提免费成立；账本变共享后前提失效，而没有任何代码或断言表达它。

后果是一类无声故障：连着 node-a 的执行端领走 node-b 的生成，增量落入 node-a 的在途缓冲，而订阅者按 id 内的 node_tag 被路由到 node-b，只看到 `Created` 随后静默。**任何路径都不报错**——与「模型什么也没产出」无从区分。

新增 L0 用例 `claim-locality`，断言外节点的排队生成既不被领取、也不被消耗。已做变异验证：移除 mem 的节点过滤后，契约立即失败并给出上述因果说明。

同时新增 `check-deps` 门禁两条：拉取端点不得回归、`claim` 必须携带 `NodeTag` 且 sql 必须真正过滤。

> 门禁第一版是**空过的**：它搜整个文件找 `node_tag = $3`，而解释该谓词的注释本身就能让它通过——移除真实谓词后仍报 OK。改为检查两处不可能出现在注释里的事实（完整 `WHERE` 子句 + `.bind(node.as_str())`），再次变异验证确认会失败。**能被自己的文档满足的门禁什么也没检查。**

### 3.3 CR-8：过载下的完整性

CR-8 验收明确要求「高压拒绝时**无双领、无丢生成、无序号分叉**」。原有四个场景只establish了「拒绝会发生」：串行创建两个响应，观察到 `Overloaded`。串行不构成高压，而**拒绝路径正是计数器被多减一次、名额泄漏的地方**，无竞争则不可见。

新增 `overload-integrity` 用例，24 个任务并发冲击 limit=3 的准入边界，断言 CR-8 真正指名的三件事：

| 断言 | 失败意味着 |
|---|---|
| 准入数恰为 limit | 超出=守卫是 check-then-act；不足=拒绝吃掉了未持有的名额 |
| 全部 racer 都得到裁决 | 请求被静默丢弃而非被回答 |
| 已准入者均可读回 | CR-8 所称「丢生成」 |
| 无响应被领取两次 | 拒绝期间的双领 |
| 序号无分叉 | 压力下准入的生成序号错乱 |

> 写这个用例时首次运行报「limit=3 只准入 2」。并非缺陷——是清场逻辑写错了：`claim` 把 queued 变为 in_progress，**两种状态都占在途配额**，须驱动至终态才释放。

### 3.4 CR-11 无法在端口层验证（缺口如实记录）

加强 CR-11 断言（改为读回已记录的用量，而非只看调用返回 Ok）后，读回值为 0。追查结论是**架构缺口，非测试问题**：

- `record_partial_usage` 把金额记入 `(response_id, attempt)` 侧表
- 读取方法 `partial_usage_count` **只存在于 mem 适配器的具体类型上，不在 `ResponseLedger` trait 内**

因此只持有端口的计费消费方**取不到已记账的部分用量**，本套件也无从校验。已从 L0 移除该声明并在代码处记录原因；CR-11 由 L1 `partial-usage-accounted` 经 trace 覆盖。

**这是一个待你决策的产品问题**：若计费确需经端口取数，`ResponseLedger` 缺一个读取方法。

---

## 4. 并发契约：顺序执行无法区分「原子」与「恰好没冲突」

`concurrency` 用例是本轮最重要的补强。此前所有断言都是顺序驱动的，而**恰好一次领取、幂等、序号分配三者都是并发性质**：check-then-act 实现能通过全部顺序测试，仍会在生产双领。

用 12 个 `tokio::spawn` 任务争抢，断言：

| 断言 | 失败意味着 |
|---|---|
| 无响应被领取两次 | 两个执行端写同一次生成，输出交织（CR-1） |
| 一个幂等键只产出一个生成 | 超时重试的调用方被重复计费（CR-2） |
| 并发 append 序号无重复 | 两条事件共享序号，`starting_after` 歧义，续订静默丢事件（INV-11） |
| 序号仍 0 基连续 | 缺口与驱逐无从区分 |

### 4.1 并发检查本身也被验证有效

`RaceyEventLog` 是故意写错的实现（read-then-increment 中间 yield）。测试断言**契约会拒绝它**。

> 这一步不能省：无法失败的并发断言比没有断言更糟——它报告了自己从未建立的安全性。没有它，`concurrency` 通过只能说明 mem 适配器恰好没有交错。

---

## 5. 裁判（Oracle）

裁判审视**整条 trace**，因此能发现单步无法观测的违规。

| Oracle | 支撑 | 捕捉的失败 |
|---|---|---|
| `SequenceContiguous` | INV-11, CR-5, CR-4 | 跳号或非 0 起始 |
| `SingleClaimPerAttempt` | CR-1, INV-1 | 双领 |
| `CreatedResponsesTerminal` | CR-6, INV-35 | 已接受但永不终态 |
| `ExpiredIsExplicit` | INV-40, CR-4, FR-12 | 声称过期却仍返回数据 |
| `StaleAppendRejected` | CR-3, INV-6, CR-7 | 失效持有者写入被接受 |
| `IdempotentSameResponse` | CR-2, INV-2 | 一个幂等键两个生成 |
| `ChainBounded` | CR-9, INV-41/42 | 成功路径上超限 |
| `NoSilentContentLoss` | CR-10, INV-43/46 | 声明存储却无确认且无显式拒绝 |
| `IntegrityVerified` | CR-13, INV-44 | 完整性校验失败 |
| `UsageAccounted` | CR-11, INV-51 | 作废 attempt 记零 token |
| `ChainClosure` | CR-12, INV-47 | 输出类型不在可接受输入集内 |

### 5.1 裁判必须声明自己判定什么（空过检测）

```rust
fn needs(&self) -> &'static str;
fn saw_relevant_data(&self, trace: &Trace) -> bool;   // 无默认实现
```

`run_oracles` 在裁判「无相关数据可判」时**报错**。

> **修复的缺陷**：绝大多数裁判写法是「扫描违规，否则通过」，因此在从未触及该性质的 trace 上**平凡通过**。例如 `IntegrityVerified` 只在记录了失败校验时才失败——一个根本没做完整性校验的场景会通过它，同时把 CR-13 计入覆盖率。这是一次虚假的验证声明，且极易通过评审。
>
> `scan_style_oracles_all_pass_vacuously_on_an_empty_trace` 把这一危险性质固定下来：若将来有人把某个裁判改成 fail-closed，该测试会失败，提示相关性检查对它已冗余。
>
> `ChainClosure` 例外——它断言类型系统性质，与 trace 无关，故恒可判定。让它依赖 trace 反而会重新引入空过。

### 5.2 未知裁判名报错而非跳过

否则场景里一个拼写错误会静默关掉一项检查。由 `unknown_oracle_name_is_an_error` 固定。

---

## 6. L1 场景（29 个）

| 组 | 场景 |
|---|---|
| 生命周期 | sequential-responses · idempotent-create · double-claim · claim-when-empty · attempt-fence |
| 流式 | resume-starting-after · event-expired-explicit |
| 上下文链 | chain-multi-turn · chain-depth-limit · chain-bytes-limit · chain-broken-explicit · chain-cross-tenant-denied · instructions-not-inherited · store-false-not-referencable |
| 存储治理 | content-delete-and-sweep · tenant-purge · integrity-tamper-detected |
| 可靠性 | orphan-reclaim-on-boot · partial-usage-accounted · context-store-down-rejects-write |
| 过载 / 降级 | pending-limit-overload · overload-reject-consistent · read-only-reject |
| 访问控制 | cancel-cross-tenant-denied |
| 协议子集 | item-reference-rejected · inline-binary-rejected · internal-url-rejected · unknown-field-rejected · chain-closure |

---

## 7. L2 场景（13 个）与节点职责

**无逐场景夹具重置**，故隔离靠节点分工 + 运行顺序：

| 节点 | 职责 |
|---|---|
| node-a (18080) | 正常流程；mock agent 挂在此 |
| node-b (18081) | 路由与优雅停机 |
| node-c (18082) | 过载与崩溃；调度器**故意挂起**（`hanging-script.yaml`），创建于此的生成永久在途 |

### 7.1 破坏性由场景声明，不由文件名承载

```yaml
destructive: true          # runner 强制排到最后
requires_nodes: [18080, 18081]
```

> **修复的缺陷**：原先靠 `zz-` / `zzz-` 文件名前缀让破坏性场景排最后。这意味着一个命名合理的新场景仍可能被排序运气搁死，**且失败信息不会提示原因**。现在顺序由 `destructive` 标志强制，前置条件不满足时报错直接说明「某个破坏性场景先跑了，夹具不会重置」，而不是在后续某条无关断言上失败。

| 场景 | 验证 |
|---|---|
| health-and-create | 三节点对等，无权威节点 |
| sync-mode-http | 同步等待返回终态对象 |
| background-then-subscribe-http | 后台创建后订阅，游标续订不重复 |
| directed-routing-resume | **经非宿主节点订阅**，`route_inflight` 透明代理 |
| multi-turn-chain-http | 服务端拼接历史；instructions 不继承 |
| delete-response-http | 删除后链断裂显式 |
| unknown-field-400-http | 严格拒绝含定向补救说明 |
| cross-tenant-404-http | 未知标识 / 非法格式 / **表外节点标签**同形 404（SEC-5） |
| read-only-http · pending-limit-http | 降级与过载可见 |
| idempotent-create-http | 幂等 |
| graceful-drain-and-no-sticky-resume 🔥 | **SIGTERM** 触发 drain；停掉转发节点后**同一游标换节点续订成功**（FR-32） |
| node-down-abrupt 🔥 | **SIGKILL** 后宿主消失，对端明确失败而非编造部分数据 |

最后两个场景的信号选择是语义的一部分：用错信号会验证相反的行为。

### 7.2 FR-32 与 FR-34 合并验证的理由

两者都需要停掉 node-b，而夹具不重启节点。合并后场景反而更有说服力：**优雅停机之后，调用方换节点仍能用同一游标续订**——这正是「接入层无粘性」的可观测后果。若订阅被钉在接受它的节点上，每次发布都会打断所有开启的流。

## 8. L3 检查（5 项 + 契约复用）

| 检查 | 只有共享持久化才能显现的性质 |
|---|---|
| `sql-port-contract` | **同一套 L0 契约跑 SQL 后端** |
| `sql-shared-store-no-forward` | `is_shared()` 为真 ⇒ 直连且链亲和退役 |
| `sql-multi-turn-chain` | 物化快照跨节点固化；**链可跨节点**（正是链亲和须退役的理由）；快照不选 instructions 列 |
| `sql-restart-history-intact` | 重启后在途明确失败、**历史完好** |
| `sql-expiry-sweep` / `sql-tenant-purge` | 到期清理与租户清除 |

---

## 9. 覆盖率门禁

`just coverage` 相对 `spec.md` v3 的 FR/CR/INV/SEC 基线统计。

**覆盖来源全部为实际运行导出**，无硬编码编号表：

| 来源 | 导出方式 |
|---|---|
| L0 契约 | `SuiteReport::covered()` — 只算真正跑过的用例 |
| L1/L2/L3 场景 | 解析 YAML 的 `covers` + 所声明裁判的 `covers()` |
| 静态门禁 | `check_protocol_spec_is_publishable()` 返回自身支撑的编号 |

门禁条件：

- **CR 必须全覆盖**，否则失败
- FR/INV 缺口若在 `deferred` 内则不卡门禁，但**仍在报告中列出**

### 9.1 deferred 列表已收敛到只剩 FR-31

复查发现原列表里 5 项其实**当时就可验证**，只是被写进了 deferred：

| 编号 | 实际归属 |
|---|---|
| FR-23 | 移入 `check-deps`（发布规范由门禁机械校验） |
| FR-32 | 移入优雅停机场景 |
| SEC-5 | 移入 cross-tenant-404-http（表外节点标签 404 且不外发请求） |
| INV-34 | 新增 L0 `durability-order` 用例 |
| INV-12 | 已由续订场景覆盖 |

只有 **FR-31** 真需要数据库（共享存储后不再转发），由 L3 覆盖。

> 这是 deferred 列表最典型的失效方式：**编号一旦写进「延后」，就没人再复核它**。因此现在的规则是——延后项必须写明「为何当期无法验证」，而非仅列编号。

### 9.2 解析器不得对合法输入静默失败

`covers` 支持行内与块式两种 YAML 数组。曾因只支持块式而静默漏统计**全部**场景声明，覆盖率严重失真。用于门禁的解析器若对合法输入静默返回空，比报错糟糕得多。

同理，场景计数会跳过纯注释的迁移墓碑文件——与 runner 行为一致，否则报告的套件规模虚高。

## 10. 单测分布（261 项）

| 位置 | 数量 | 侧重 |
|---|---|---|
| `crates/core` | 102 | 协议子集拒绝面、**规范化属性测试**、出站 completions 翻译、标识校验 |
| `crates/adapters/mem` | 27 | 环驱逐水位、快照读取、租户索引、过期堆 |
| `crates/adapters/sql` | 2 | 连通性与迁移可加载；无数据库时跳过而非失败 |
| `crates/adapters/completions-mock` | 17 | 脚本匹配、切分无损、失败形态可区分 |
| `crates/agent` | 11 | **引擎端到端**：领取→流式→提交，含栅栏与坏结果，无 socket 无模型 |
| `crates/gateway` | 74 | 配置校验、鉴权与内部头、错误映射、48 项 HTTP 契约 |
| `testing/conformance` | 9 | **契约元测试**：分发完整性、空过检测、并发变异验证 |
| `testing/harness` | 19 | **裁判元测试**：空过拒绝、未知名报错、相关性声明完整 |

HTTP 契约测试直接编译网关模块（`#[path]`），因此验证的是二进制实际挂载的同一个 router。

### 10.1 引擎测试为何值 11 项

内部执行把整条执行路径变成进程内可测（D23）。`crates/agent/tests/engine_end_to_end.rs` 用内存适配器加 mock 调度器覆盖：完成、空队列空转、**外节点工作不被执行**、调度失败收口、坏结果拒绝、拒答仍存储、`store=false`、挂起、启动排水、服务端组装历史、链断裂失败。

全部无 socket、无数据库、无模型——这是这层抽象最直接的回报。

### 10.1 规范化改为穷举切分点

原测试取 3 个手挑切分点。那足以捕获它针对的那个错误，但**不足以建立该性质**：失败依赖边界位置，真正危险的切分恰是没人想到的那个。

现在对每个输入穷举**全部字符边界**的两段切分，外加逐字符最大碎片化，覆盖组合标记、ZWJ 表情、多字节等 8 类输入。

同时补上反向断言：**不同文本不得折叠为同一指纹**。只测「等价形式一致」会漏掉一类归一化缺陷——它会让篡改无法检出。
