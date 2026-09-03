# 协议子集规范（对外可发布）

> 版本：v1.0 · 依据 [D22](../architecture/decisions.md#d22-协议封闭子集与严格拒绝)
> 上游依据：`github.com/openai/openai-openapi`，修订 `2025-08-07`
> 面向：接入本服务的调用方

---

## 0. 一句话

本服务实现 OpenAI Responses 协议的**封闭子集**。子集内行为与上游一致；**子集外一律返回 `400`，不静默忽略**。

严格拒绝未知参数**恰好也是上游行为**，因此严进同时更安全、更兼容——这里不存在取舍。

---

## 0.1 术语对照（与 OpenAI 官方对齐）

为避免与 OpenAI 官方文档沟通时产生分歧，本文档与代码统一使用 OpenAI 官方命名。关键术语对应如下：

| 本文档/代码术语 | OpenAI 官方 | 取值 |
|---|---|---|
| 响应（Response） | Response | `POST /v1/responses` 创建的对象 |
| 条目（Item） | Response Item | `message` / `function_call` / `function_call_output` |
| 内容片段（Content Part） | Content Part | `input_text` / `output_text` / `refusal` / `input_image` / `input_file` |
| 角色（Role） | Role | `user` / `assistant` / `system` / `developer` |
| 条目状态 | Item Status | `in_progress` / `completed` / `incomplete` |
| 响应状态 | Response Status | `queued` / `in_progress` / `completed` / `failed` / `incomplete` / `cancelled` |
| 上一响应 | `previous_response_id` | 串联多轮上下文 |
| 用量 | Usage | `input_tokens` / `output_tokens` / `total_tokens` |
| 事件 | Streaming Event | `response.created` / `response.output_text.delta` / `response.output_item.added` / … |
| 游标 | `sequence_number` / `starting_after` | 0 基连续，SSE `id:` 承载 |

> **内部概念，无 OpenAI 对应**——以下术语仅在本服务内部使用，不出现在对外协议里，讨论时请勿与 OpenAI 术语混用：

| 内部术语 | 含义 |
|---|---|
| 物化快照（materialised snapshot） | 创建时把祖先条目扁平拷贝进 `context`（D24） |
| 在途缓冲（in-flight buffer） | 共享载体上有界的事件缓冲（Redis Streams / mem-server），瞬态 |
| 栅栏（fence / attempt） | 防止并发或过期写入的尝试号 |
| 领取（claim） | 执行端从共享 ledger **全局**认领待执行响应 |

---

## 1. 支持的请求参数

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `model` | string | 必填 | |
| `input` | string \| 条目数组 | 必填 | 字符串是「单条 user 消息」的简写 |
| `instructions` | string | — | 插入上下文最前的 system/developer 消息；**不跨轮继承** |
| `store` | bool | **`true`** | 是否持久化本次条目 |
| `stream` | bool | `false` | 同连接 SSE |
| `background` | bool | `false` | 立即返回，另行订阅 |
| `previous_response_id` | string | — | 上一环标识；服务端据此拼接历史 |
| `max_output_tokens` | int > 0 | — | |
| `metadata` | map<string,string> | — | ≤ 16 项，键 ≤ 64B，值 ≤ 512B |
| `tools` | 数组 | — | 仅 `type: "function"` |
| `tool_choice` | `"auto"` \| `"none"` \| `"required"` \| `{type:"function",name}` | — | |
| `temperature` | 0–2 | — | |
| `top_p` | 0–1 | — | |

### 1.1 明确拒绝的参数（附替代方案）

| 参数 | 错误 | 替代 |
|---|---|---|
| `conversation` | `400 unsupported_parameter` | 用 `previous_response_id` 串联 |
| `context_management` | `400 unsupported_parameter` | 自行控制链长；超限会明确报错 |
| `prompt` | `400 unsupported_parameter` | 直接传 `instructions` 与 `input` |

其余未列出的上游参数返回 `400 invalid_request`，错误信息**指明字段名**。

---

## 2. 条目类型

| 类型 | 支持 | 备注 |
|---|---|---|
| `message` | ✅ | `role`: user / assistant / system / developer |
| `function_call` | ✅ | `arguments` 为不透明字符串，原样保存 |
| `function_call_output` | ✅ | |
| `item_reference` | ❌ | **见 §2.1** |
| `reasoning` | ❌ | 加密推理内容需原样往返，不在子集内 |
| `computer_call` / `mcp_*` / 托管工具 | ❌ | 产品范围外 |

### 2.1 为何拒绝 `item_reference`

它允许按标识引用任意历史条目，从而**绕过走链时的逐环租户校验**。这是一条越权读取路径，因此拒绝它是安全属性而非范围取舍。即使将来扩展子集，也须先解决引用条目的归属校验。

---

## 3. 内容片段

| 类型 | 支持 | 约束 |
|---|---|---|
| `input_text` / `output_text` / `refusal` | ✅ | |
| `input_image` | ✅ | `file_id` 或 **https** `image_url`；二者至少其一 |
| `input_file` | ✅ | `file_id` 或 **https** `file_url` |
| 内联 base64 | ❌ | 见 §3.1 |

### 3.1 为何只接受引用

两个原因，第二个更重要：

1. 请求体体积失控
2. **引用是短字符串，上下文链的 1 MiB 字节上限才保持有效**。若允许内联，单张图片即可撑满预算，上限形同虚设

### 3.2 链接限制（SSRF 纵深防御）

执行端会去拉取这些链接，因此网关入口即校验：

- 仅 `https`（因此 `data:` URL 被同一规则拦下）
- 拒绝内网与保留地址段：`9./10./11./21./30./127./169.254./172.16-31./192.168./100.64-127.`、`0.0.0.0/8`、`240.0.0.0/4`、文档段（`192.0.2./198.51.100./203.0.113.`）、IPv6 `::1`/`fc00::/7`/`fe80::/10`
- 拒绝 `localhost`、`.local`、`.internal`、`.intranet`、`.corp`、`.lan` 等后缀
- 拒绝 URL 内嵌凭据；长度 ≤ 2048B

> **这是第一层防护，不是全部。** 主机名在拉取时解析到内网地址（DNS 重绑定）需由拉取方在出网时再次校验。

### 3.3 文件标识归属不在本服务边界内

`file_id` 的归属校验**由文件服务负责**。本服务只校验上下文链各环的租户归属。不写明会让跨租户 `file_id` 引用成为无主风险。

---

## 4. 请求体限制

| 项 | 上限 |
|---|---|
| 条目数 | 200 |
| 单条目字节 | 256 KiB |
| 总字节 | 1 MiB |
| JSON 嵌套深度 | 32 |
| `instructions` | 32 KiB |

---

## 5. 多轮上下文

```
第 1 轮：{"model":"m","input":"我叫 Ada"}                        → resp_A
第 2 轮：{"model":"m","input":"我叫什么","previous_response_id":"resp_A"}
```

**请求体不随轮次增长**：第 5 轮与第 1 轮体积相同，调用方只需持有一个标识。

### 5.1 四类链断裂（均为 `400`，绝不静默降级）

| 情形 | 错误码 |
|---|---|
| 环缺失 / 已过期 / 已删除 / 跨租户起点 | `chain_broken` |
| 被引用者 `store: false` | `previous_not_stored` |
| 超深度上限（默认 50） | `chain_too_long` |
| 超字节上限（默认 1 MiB） | `chain_too_large` |

**不会发生的事**：静默截断上下文、静默退化为单轮。这两者都会表现为「模型突然失忆」，从外部几乎无法归因。

### 5.2 `instructions` 不跨轮继承

与上游语义一致：`instructions` 是响应对象上的字段，不是条目；**与 `previous_response_id` 一起使用时，上一轮的 `instructions` 不被带入**。

若需人格持续生效，**每轮都要重传**。

---

## 6. 事件与续订

事件名：`response.created` / `response.in_progress` / `response.output_text.delta` / `response.output_item.added` / `response.output_item.done` / `response.function_call_arguments.delta` / `response.function_call_arguments.done` / `response.completed` / `response.failed` / `response.incomplete`。

流式序列与上游一致，分文本回答与工具调用两类：

**文本回答**

1. `response.output_item.added`（`message`，`item` 承载条目，含 `id`）
2. `response.output_text.delta`（`delta` 为增量，`item_id` 为 message 的 `id`，可多次）
3. `response.output_item.done`（`message`）

**工具调用**

1. `response.output_item.added`（`function_call`，参数为空）
2. `response.function_call_arguments.delta`（`delta` 为参数增量，`item_id` 为 `call_id`，可多次）
3. `response.function_call_arguments.done`（`arguments` 为完整参数，`item_id` 为 `call_id`）
4. `response.output_item.done`（`function_call` 完成）
5. 工具执行后：`response.output_item.added` / `response.output_item.done`（`function_call_output`）

事件对象字段：`type`、`sequence_number`，以及按类型分发的 `delta` / `item` / `arguments`；`output_index` 标注条目在 `output` 数组的位置，`item_id` 把增量关联到条目。`response_id` 与 `attempt` 是内部字段，不出现在线上。

游标字段 `sequence_number`，**0 基连续**。SSE `id:` 承载该值，故 `Last-Event-ID` 可直接用于续订。

`starting_after` **排他**：`starting_after=N` 返回 `N+1` 起。省略该参数表示从头（含 `sequence_number: 0`）。

**位点已驱逐 → `410`，无恢复路径。** 订阅是瞬态能力：终态后仅保留可配置时长（默认 60s）。需要完整渲染历史的调用方**必须自行从实时流构建并持有**——服务不提供事件历史查询。

---

## 7. 调用方职责

| 项 | 归属 |
|---|---|
| 完整事件历史（transcript） | **调用方**。服务只缓冲在途事件 |
| 渲染中间态（思考过程、工具进度） | **调用方**从实时流自取。`GET` 返回生成对象的**当前状态**：未完成时返回 `status: in_progress` 的部分对象（**不失败**），终态返回完整对象 |
| `file_id` 归属 | **文件服务** |
| 上下文长度控制 | **调用方**。超限明确报错，服务不自动压缩 |
| 断线续订游标 | **调用方**持有 `(response_id, sequence_number)` |

---

## 8. 扩展流程

子集是**契约**，扩展需显式变更：

1. 提出用例 → 评审是否纳入子集
2. 若涉及新条目类型，先确认**链闭合性**：输出类型必须落在可接受输入类型集合内，否则本服务自身的上下文链会断
3. **网关先行升级**（先放宽接受面，再升执行端），否则线上出现 `400` 突增
4. 更新本文并声明新的上游依据修订

CI 会定期拉取上游做字段级 diff 并**告警**。该检查的性质是「是否扩展子集」的产品决策输入，**不是正确性门禁，不阻塞构建**——混淆这一点会导致上游每次发版都堵住流水线，最终被禁用而失去告警能力。
