# nova-sessions

Nova 产品线下的 **Session 级可回放消息服务**：多区域建会话、异步 Turn、跨 Turn 流式观测与续订。

> 产品范围见 [D19](docs/architecture/decisions.md#d19-产品范围为-session-流式-api单门面)。本仓库**不是**分布式任务 / 容量调度系统。

## 快速开始

```bash
just verify          # 全量自动验证：L0 + L1 + L2
just unittest        # 单测
just sim             # 人工冒烟 → http://127.0.0.1:19090 （Chat：/chat）
```

## 命令（按意图）

### 验证 / 测试（CI 门禁）

| 命令 | 作用 |
|------|------|
| `just verify` | 依次跑完 L0→L1→L2（默认） |
| `just verify l0` | 端口契约（conformance mem suite） |
| `just verify l1` | MemWorld 场景 + Trace / Oracle |
| `just verify l2` | 本机多进程 HTTP 场景 |
| `just unittest` | workspace lib 单测（xtask；有子用例才打印） |
| `just coverage` | 相对 FR/CR/INV 基线的覆盖**结论**（缺口列表；详情见 reports） |
| `just check-deps` | core 不得依赖 adapters / gateway |

### 开发夹具

| 命令 | 作用 |
|------|------|
| `just sim` | 冒烟控制台 `/` + Chat `/chat`（**非**正确性门禁） |
| `just procs up\|down` | 手动起停 L2 进程 |

### 部署

| 命令 | 作用 |
|------|------|
| `just deploy …` | Docker Compose（可选；D17） |

## 仓库结构

```text
.
├── crates/
│   ├── gateway/              # nova-sessions-gateway — HTTP(S) 接入
│   ├── core/                 # nova-sessions-core — 领域 + 协议
│   └── adapters/
│       └── mem/              # adapters-mem — 一期默认内存实现
├── testing/
│   ├── harness/              # Trace + Oracle + L1/L2 场景 runner
│   ├── conformance/          # L0 端口契约
│   ├── mock-agent/           # 假 Agent
│   ├── sim/                  # 人工冒烟（非 CI 门禁）
│   ├── scenarios/            # L1/L2 YAML
│   ├── config/               # 本机 L2/sim 拓扑
│   └── reports/              # Trace / coverage 输出
├── xtask/                    # 门禁编排：verify / unittest / coverage / procs / deploy
└── docs/                     # 需求 · 架构 · 设计 · 计划
```

## 文档入口

从 [`docs/README.md`](docs/README.md) 开始：需求 → D19/D11 → [`design/01-session-stream.md`](docs/design/01-session-stream.md)。
