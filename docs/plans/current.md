# 当期计划：mem 驱动主框架完备（延后真实 ports）

> 状态：**展开级**
> 前置：V1–V9 已完成（验证先行 + Session 流最小闭环，coverage 基线 100%）
> 设计：[`../design/01-session-stream.md`](../design/01-session-stream.md) · [`../design/02-verification.md`](../design/02-verification.md)
> 原则：**用 mem 扩展证明主框架能力完备**；真实存储 / 网络适配器 **尽量延后**

---

## 0. 本轮定位

| 要证明的 | 不在本轮证明的 |
|----------|----------------|
| 主框架在**端口契约**下行为自洽（控制面、不变量、恢复路径） | JetStream / Redis / DB 等真实引擎的性能与故障语义 |
| mem 是**验证引擎**，缺口按框架语义扩展 | 把 mem 打磨成「准生产集群」本身 |

**判据**：每项交付必须落到 `core` / `ports` 语义 / `gateway` 行为 + L0–L2 Oracle；禁止「只在 mem 私货里开关、框架路径走不到」。

---

## 1. 已锁定（继承）

| 项 | 出处 |
|----|------|
| 产品 = Session 可回放消息；gateway POST+SSE | D19 |
| meta ≠ stream | D11 |
| 验证先行：Trace + Oracle；L0–L2 mem | D15 · D17 |
| 真实 ports 接入 **延后**（本轮不选引擎、不上 Docker 存储） | 本计划 |

---

## 2. 交付与验收（V10–V13）

| # | 交付 | 框架缺口 | mem / 验证怎么证 | 验收 |
|---|------|----------|------------------|------|
| **V10** | **热 miss → 快照 / 冷层恢复闭环** | FR-13/14 · INV-14 | mem：可 trim 热层 + 内存「冷段」；Gap 必须带 hint；gateway/SSE：409 + 客户端改走 snapshot 再 `from_seq` | ✅ L1 `hot-miss-gap` + L2 `hot-miss-recover-http` |
| **V11** | **开屏契约产品化** | FR-9 · INV-13 | 统一「snapshot → SSE from snapshot_seq」；中途开屏不依赖全量热回放 | ✅ `stream_from_seq` + L1 `mid-snapshot` + L2 `open-screen-mid-http` |
| **V12** | **只读 Mirror 语义（进程内）** | FR-10 · 读路径 · INV-32 联动 | mem：权威写 + 异步/同步投影到 mirror 视图；mirror **拒写**；双观察者同序 | ✅ `MemMirrorView` + L1 `mirror-dual-read` + L2 `edge-read-mirror` |
| **V13** | **多接入 / 无粘性拓扑强化** | FR-17 · INV-12 | 多 gateway 共一份 MemWorld（测试夹具内共享）；杀实例后续订；禁止连接级游标 | L2 多 listen + kill + 游标续订；扩展现有 stop-edge |

可选加深（不挡 V10–V13 退出）：

| # | 交付 | 说明 |
|---|------|------|
| **V14** | 场景化压力（mem） | 多 Session × 多 Agent × 过载拒绝 + Oracle；**仍不是**生产压测平台 |
| **V15** | 客户端续订契约文档 / 参考实现 | 固化 `JitteredBackoff` + `(session_id, last_seq)`；SDK 独立成包可再后移 |

---

## 3. 明确延后（ports 接入）

以下 **本轮不做**（除非验证发现端口形状必须改，只改 **trait / 错误类型**，不绑实现）：

- Redis / JetStream / 云厂商流存储选型与落地  
- 跨机真实 Mirror、跨区 RTT、磁盘冷层  
- 多物理 home 共享存储集群  
- Docker 依赖的验证门禁（保持 D17）

**允许的「像 ports 的东西」**：仅 mem 内模块（如 `MemHot` / `MemCold` / `MemMirror`），对外仍只暴露现有或微调后的 `StreamChannel` / `SnapshotStore` / `MetaStore`。

---

## 4. 工作方式

1. **先改契约与 gateway，再填 mem**——避免适配器私货。  
2. 每项 V 必须：`covers` + Oracle（或 L0）+ `just verify` 绿。  
3. `just coverage` 继续作需求仪表盘；新增能力同步基线 ID。  
4. `sim` 仅冒烟，不进正确性门禁。

---

## 5. 退出标准

- V10–V13 全绿；文档写清「证的是框架契约，不是存储引擎」  
- 真实 ports 仍可零实现开工：新适配器只需过 **同一 L0 契约套件**  
- 计划归档本轮后，下一期才进入「选引擎 + 适配器」入口级计划

---

## 6. 上一轮摘要（V1–V9，已完成）

验证先行 · gateway/mem 闭环 · Trace/Oracle · FR-17 续订 · INV-32 只读 · 过载/pending_limit · INV-33 退避 · coverage 基线 100%。详见本文件历史提交与 changelog。

---

## 7. 变更日志

| 日期 | 变更 |
|------|------|
| 2026-08-27 | **V12**：`MemMirrorView` 拒写投影；edge GET 读 mirror；L1/L2 双读 |
| 2026-08-27 | **V11**：`stream_from_seq` 开屏契约；append 抬 tip；L1/L2 mid-open |
| 2026-08-27 | **V10**：mem hot/cold trim + admin `trim_hot`；L1/L2 409→snapshot→续订 |
| 2026-08-27 | **新当期**：mem 驱动主框架完备；真实 ports 延后；规划 V10–V13 |
| 2026-08-27 | 结项 V1–V9（见上节摘要） |
