# L4 — 官方 SDK 兼容层

验证 D27 的「官方客户端可无改接入」这一主张本身。这里不做任何 HTTP 手工拼包，
只用官方 `openai` Python SDK 的 `client.conversations.*` 与 `client.responses.create`。

## 运行

```sh
just verify l4          # 缺少 python3 / openai 包时自动 SKIP，绝不失败
```

需要：

- `python3` 在 PATH 中
- `pip install -r testing/sdk-compat/requirements.txt`

## 自跳过约定

本层是**补充验证**而非硬性门禁：开发机可能没有 Python 或无法安装 SDK 包，此时
`verify l4` 打印 `SKIPPED` 并退出 0。`run.py` 一旦能跑起来，其内每条断言都必须
成立——跳过只发生在「跑不起来」这个层面，不存在「跑起来但部分断言跳过」。

## 边界

- 只验证**协议形状**：官方 SDK 发什么、我们回什么能否被 SDK 解析。租户鉴权在本地
  夹具是空表（未鉴权）模式，与生产无密钥配置一致（SEC-4 要求生产必配密钥）。
- 不验证模型行为：`responses.create` 由 L2 夹具的 scripted mock 模型完成。
