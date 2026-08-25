# 迭代 0（展开级）：验证框架先行

> 状态：执行中
> 退出条件：`just verify l0`、`just verify l1`、`just verify l2` 全绿；`just procs up` 可起停 1 home + 2 edge；`just coverage` 可生成矩阵（见 §5 门禁策略）

---

## 1. 目标

1. 需求基线已含三类任务画像与 A6 校准（见 `requirements/`）
2. Rust workspace：端口化骨架 + 内存适配器
3. 分层验证：conformance（L0）+ testkit 场景/裁判（L1）+ 本机多进程（L2）
4. 部署轨骨架（Docker）与验证轨分离

**业务实现可为骨架**；验证用例须能对未来实现给出红/绿判定。

---

## 2. 模块与依赖

```
nova-core →（被）nova-ports → matcher / claim / adapter-mem / conformance / testkit
nova-server 装配 matcher + claim + adapter-mem
nova-mock-worker 执行侧 mock
```

职责边界见仓库计划与 crate 内文档注释。库 ≠ 服务；仅 `nova-server`、`nova-mock-worker` 为二进制。

### 生产端口（签名意图）

| 端口 | 要点 | INV / 决策 |
|------|------|------------|
| `TaskStore` | 唯一 `try_claim`，无 get+update | INV-1/4/20 |
| `CapacityLedger` | 服务端核算 | D10、INV-3 |
| `IdempotencyGate` | `reserve`，无 TTL 参数 | INV-2 |
| `StreamChannel` | 与 store 路径分离，内部分配序号 | D11、INV-11 |
| `Clock` | `now` / `sleep_until`；测试可推进 | D15 |
| `PolicySandbox` | 投影入参、有界求值 | INV-18/19、SEC-6 |
| `MetricsSink` | 薄计数；测试可读 | OR-3 |

`FaultInjector` **不**进入 ports。

---

## 3. 验收命令与频率

| 命令 | 含义 | 建议频率 |
|------|------|----------|
| `just verify l0` | conformance + crate 单测 | 每次编辑 |
| `just verify l1` | 进程内场景 + 虚拟时钟 | 每次编辑 / PR |
| `just verify l2` | 本机多进程场景 | PR / 日构建 |
| `just procs up\|down` | 起停 home+edge+mock-worker | 手工 / L2 |
| `just coverage` | 需求覆盖矩阵 → `reports/traceability.md` | PR |
| `just check-deps` | 分层依赖箭 | PR |
| `just deploy up\|down` | Docker（可选） | 有 Docker 的环境 |
| `just sim` | 可视化人工验收台（区域/服务/Worker/下发） | 人工演练 |

控制台：`nova-sim` → http://127.0.0.1:19090 。区域与 Worker 均为 Mock 进程；Edge 提交转发至 Home，领取仅在 Home。

---

## 4. 场景与拓扑（只引用，不抄清单）

- L1：`scenarios/l1/`
- L2：`scenarios/l2/`
- 本机配置：`config/home.toml`、`config/edge-b.toml`、`config/edge-c.toml`

裁判器（跨层复用轨迹）：`NoDoubleClaim`、`NoOversell`、`AllTerminal`、`StarvationBound` 等；完整列表以实现与 coverage 注册为准。

---

## 5. 覆盖门禁策略（迭代 0）

- `just coverage` **必须成功运行**并写出 `reports/traceability.md`
- 迭代 0 允许矩阵中存在**已登记缺口**（尚未有场景覆盖的远期需求）
- 门禁：若存在**未登记**的覆盖声明错误（场景 `covers` 引用不存在的编号）则非零退出
- 迭代 1 起收紧：当期声称覆盖的需求缺口非零即失败

---

## 6. 遗留问题

| # | 项 | 归属 |
|---|----|------|
| 1 | 真实存储 / 流通道适配器 | 迭代 1+ |
| 2 | L3 全量最终栈 overlay | 有真实适配器后 |
| 3 | A6 占比待产品确认 | parameters 校准 |
| 4 | observation / security 草稿改写 | 迭代 3 / 5 |
| 5 | 部分裁判器（如 AuthOpacity）可先占位 | 随鉴权迭代补全 |

---

## 7. Crate 一览（非服务）

| Crate | 形态 | 职责 |
|-------|------|------|
| `nova-core` | 库 | 纯类型与状态机 |
| `nova-ports` | 库 | 生产 trait |
| `nova-matcher` | 库 | 两层匹配 |
| `nova-claim` | 库 | 领取协议编排 |
| `nova-adapter-mem` | 库 | 内存适配器 + 沙箱解释器 |
| `nova-conformance` | 验证库 | 端口一致性套件 |
| `nova-testkit` | 验证库 | 时钟/故障/场景/裁判 |
| `nova-server` | **bin** | 装配 + home/edge 开关 |
| `nova-mock-worker` | **bin** | mock 执行侧 |
