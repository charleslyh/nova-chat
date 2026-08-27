# 02 — 自动化验证（Trace + Oracle）

> 状态：✅ **实现依据**（验证先行）
> 覆盖：D15 · D17 · INV/CR 可回归验收
> 配套：[`testing/harness`](../../testing/harness/) · [`testing/scenarios`](../../testing/scenarios/)

---

## 1. 目标

在 mock 支持下，对 **单能力 / 多模块集成 /（后续）压力·HA·灾备** 做 **全自动** 验收。  
`sim` 仅人工冒烟，**不**作为正确性门禁。

每条自动化场景的验收至少三层（能下沉则下沉）：

| 层 | 验什么 | 例 |
|----|--------|-----|
| **A. 直接交互** | API / 端口调用的即时结果 | HTTP 202、`SubmitOutcome::Busy`、`StreamError::Gap` |
| **B. 可控 mock 状态** | 测试双的可观测内部态 | lock=Idle、attempt、snapshot_seq |
| **C. 链路 Trace** | 各模块写入的关键事件/数据 | 提交→claim→append→完成 的因果与不变量 |

> C 是对「只断言最后一个 HTTP 响应」的补强：多模块协作时，中间态与跨模块约定必须可复盘、可裁判。

---

## 2. Trace

### 2.1 形态

- **逻辑**：有序事件流 `TraceEvent`（可 serde）
- **落盘**：默认 **统一 JSONL**（一场一文件）；L2 多进程可向同一文件 **追加**，或按组件分文件再由 harness **合并排序**（`at_ms` + 序号）
- **生产代码不写 Trace**；仅验证夹具、instrumented mock、测试专用 hook 写入（D15：故障/观测注入不进生产端口）

### 2.2 事件类别（Session 域）

| 类 | 用途 |
|----|------|
| `session_*` / `turn_*` | meta 生命周期 |
| `stream_*` | append / read / gap |
| `snapshot_*` | 开屏快照 |
| `api_*` | 对外 HTTP（L2） |
| `mock_*` | mock 组件状态快照 |
| `fault_*` | 注入记录 |
| `clock_*` | 虚拟时钟推进 |

事件须带：`at_ms`（虚拟或墙钟）、可选 `session_id` / `turn_id` / `attempt` / `seq`。

### 2.3 谁写 Trace

| 层 | 写入方 |
|----|--------|
| L0 | 通常不写；直接端口断言 |
| L1 | harness 在步骤中记录；MemWorld 侧可包一层 recording 装饰 |
| L2+ | gateway/agent **测试构建** 或旁路 proxy 追加 JSONL；或 harness 根据 API 观察合成 |

---

## 3. Oracle（裁判）

```text
Oracle::judge(trace) -> Pass | Fail { evidence }
```

- 场景 YAML 声明 `oracles: [SeqMonotonic, …]` 与 `covers: [CR-…, INV-…]`
- 裁判 **只读 Trace**（可另调 mock getter 做 B 层，但跨模块不变量优先用 Trace）
- 失败时保留 JSONL，便于复盘

内置起步集合：

| Oracle | 意图 |
|--------|------|
| `SeqMonotonic` | 同 Session 的 stream append seq 严格递增 |
| `SingleClaimPerAttempt` | 同一 `(turn_id, attempt)` 至多一次成功 claim |
| `SubmittedTurnsTerminal` | 已提交 Turn 最终有 terminal 事件 |
| `NoSilentGap` | trim 后若 `from_seq < earliest` 必须 `gap=true` |
| `StaleAppendRejected` | Trace 中至少一次 `stale_attempt` 拒绝 |
| `IdempotentSameTurn` | 同 idempotency key 映射同一 turn，且出现 duplicate |

---

## 4. 分层与断言下沉（D15）

```text
L0  契约（conformance）     — 无 Trace 亦可
L1  同进程 + MemWorld + YAML — Trace + Oracle + mock 状态
L2  多进程 HTTP/SSE         — API + 共享 Trace 文件 + mock-agent
L3  真栈（可选）            — 同 Oracle，换适配器
```

压力 / HA / 灾备 = **带标签的场景包**（fault + 负载参数），仍走 Trace+Oracle，不另起哲学。

---

## 5. 场景文件

```yaml
name: resume-stream
covers: [FR-8, FR-11, INV-12]
oracles: [SeqMonotonic, SubmittedTurnsTerminal]
trace: true   # 写入 testing/reports/traces/<name>.jsonl
steps:
  - action: create_session
  - action: submit_turn
    ...
```

步骤内可夹 **A**（expect_*）与 **B**（expect_lock 等）；结束后跑 **C**（oracles）。

---

## 6. 明确不做（本期）

- 生产路径默认打开 Trace（避免噪音与泄露）
- 用 `sim` UI 代替 CI 断言
- 把 Oracle 做成通用复杂规则引擎（保持 Rust trait + 少量内置）

---

## 7. 验收

- `just verify`：全量 L0→L1→L2
- `just verify l0`：契约绿
- `just verify l1`：场景步骤 A/B 通过 + Oracle 全 Pass + Trace 落盘
- 后续 L2：共享 Trace 下至少一条跨区/杀进程场景
