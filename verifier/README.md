# Verifier —— 端到端验证

一个独立的验证工具，用于手动验证整个服务：会话列表、会话跨轮对话、流式输出。

## 原则：验证结构与能力服务分离

- **能力服务**（`crates/gateway`、`crates/agentd`、`crates/core`、`crates/adapters/*`）
  不包含任何验证专用代码。本目录只通过它们的公开接口驱动：
  - 页面 → gateway 的公开 HTTP API（`/v1/sessions`、`/v1/responses`、…）
  - agentd → `--scheduler http`（真实 chat completions 端口实现
    `adapters/completions-http`，与 `completions-mock` 并列，属能力而非验证）
- **验证结构**（本目录）只有 `index.html` 一个页面 + `run.sh` 一个启动脚本，
  二者都不进入能力服务的编译产物。

## 快速开始

```sh
export NOVA_CHAT_BASE_URL="https://api.openai.com/v1"   # 你的 chat completions base url
export NOVA_CHAT_API_KEY="sk-..."                        # 你的 api key
export NOVA_CHAT_MODEL="gpt-4o"                          # 可选：固定模型名

./verifier/run.sh
```

然后打开 http://127.0.0.1:8080

`run.sh` 会依次拉起 mem 后端的 mem-server、sweep、agentd（`--scheduler http`）、
gateway，以及一个 `python3 -m http.server` 静态服务承载页面。Ctrl+C 全部停止。

> 需要 Python3（仅用于静态页面服务）。没有 Python3 也可以：直接用浏览器打开
> `verifier/index.html`（gateway 已开 permissive CORS，跨域可直连）。

## 页面上能做什么

| 功能 | 走的端点 |
|---|---|
| 会话列表 / 新建 / 删除 | `GET/POST/DELETE /v1/sessions` |
| 查看某会话完整历史 | `GET /v1/sessions/{id}/transcript` |
| 跨轮对话 | `POST /v1/responses { conversation, input, stream:true }` |
| 流式打字机 | 解析 `response.output_text.delta` SSE 增量 |

每新建一个会话会绑一个 conversation；在同一会话里连续发消息即跨轮对话——第二轮起
模型能看到之前的历史（服务端据 conversation 链尾指针拼上下文）。

## 一个关键的对应关系

- `NOVA_CHAT_MODEL` 未设置时：页面底部 `model` 框的值会随每个 `POST /v1/responses`
  一路传到 agentd，再由 `adapters/completions-http` 用**这个 model** 调真实
  chat completions。
- `NOVA_CHAT_MODEL` 已设置时：它优先，页面的 model 框可留空（会被覆盖）。
