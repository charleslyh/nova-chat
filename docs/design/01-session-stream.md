# 01 — Session 流与会话 API

> 状态：✅ **实现依据**
> 覆盖：FR-1~18、CR-1~8 · INV-1/2/5/6/10~16/29~35 · D19 / D11 / D18 继承 · D14 / D17
> 素材：conversation / observation 草稿已并入本文并归档（见 `docs/archive/`）。

---

## 1. 目标

实现 Session 级可回放消息服务的最小闭环：

- **`nova-sessions-gateway`**：建 Session、发 Turn、快照、SSE（写读同进程）
- `nova-sessions-core`：领域 + 协议；`adapters/mem`：实现
- mock Agent pull + reaper（attempt fence）

---

## 2. 资源与端点

| 方法 | 路径 | 语义 |
|------|------|------|
| `POST` | `/v1/sessions` | 建 Session → 201 `{session_id}` |
| `POST` | `/v1/sessions/{id}/turns` | 发 Turn → 202 `{turn_id}`；可选 `stream=true` 同响应 SSE |
| `GET` | `/v1/sessions/{id}/snapshot` | 开屏快照 `{state, snapshot_seq}` |
| `GET` | `/v1/sessions/{id}/stream?from_seq=` | SSE 增量；`Last-Event-ID` 可映射为 from_seq |

外区：POST 转发权威区；GET 回源 stream/snapshot。

客户端游标：`(session_id, last_seq)`。无 sticky。

---

## 3. 域事件（方案 A：同 Session 流）

| 事件 | 可合并 | 说明 |
|------|--------|------|
| `turn_begin` | 否 | 含 user_message |
| `session_busy` / `session_idle` | 否 | 锁可见性 |
| `attempt_started` | 否 | claim 后 |
| `text_delta` | 是 | token |
| `turn_done` / `turn_failed` | 否 | 终态 |
| `attempt_aborted` | 否 | reaper |

每条事件：`session_id`、`seq`、可选 `turn_id` / `attempt` / `message_id`。

快照：`{ bubbles[], running[], snapshot_seq }`；`snapshot_seq` 单调。

---

## 4. 端口

### StreamChannel

```text
append(event) -> seq
read_from(session_id, from_seq, limit) -> Ok(events) | Err(Gap { hint })
```

热层缺 `from_seq` → **明确 Gap**，禁止空 Vec 冒充「没有」。

### SnapshotStore

```text
put(session_id, snapshot)  // 拒绝回退 snapshot_seq
get(session_id) -> Option<Snapshot>
```

### MetaStore（最简）

Session 行、锁 CAS（idle↔busy）、pending Turn、attempt、Agent 存活。

```text
create_session() -> session_id
submit_turn(session_id, text, idempotency_key) -> turn_id | Busy | Duplicate
claim_turn(agent_id) -> Option<(turn, attempt, deadline)>
complete_turn(turn_id, attempt) -> bool
heartbeat(agent_id)
reap() // 超时回收
```

无容量账本、无匹配器。

---

## 5. 时序要点

1. **开屏**：GET snapshot → SSE `from_seq=snapshot_seq`
2. **Turn**：POST → CAS busy + pending + append turn_begin → 202；Agent claim → append deltas → done + idle
3. **fence**：reaper 抬 attempt → 旧 append 拒绝
4. **热 miss**：SSE 返回 409 + recover_hint

---

## 6. 明确不做

容量匹配、任务列表、Gateway/Realtime 两服务、Mirror（一期）、并行 Turn、完整鉴权票。

---

## 7. 验收

见 [`../plans/current.md`](../plans/current.md) 与 `just sim`。
