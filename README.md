# nova-responses

A generation service exposing a **closed subset of the OpenAI Responses protocol**:
create a response, receive it synchronously / streamed / in the background, and
resume a stream by sequence number. Conversation history is retrieved server-side
from an identifier, so callers never resend it.

> Scope: [D20](docs/architecture/decisions.md#d20-交付边界收口存储与订阅分离) ·
> [D21](docs/architecture/decisions.md#d21-可靠性分层三类存储的差异化投入) ·
> [D22](docs/architecture/decisions.md#d22-协议封闭子集与严格拒绝).
> This is **not** a task or capacity scheduling system.

## What is and is not stored

The single most important thing to understand before integrating:

| Data | Stored? | Where |
|---|---|---|
| A response's **input and output items** | **Yes** (`store: true` by default) | Durable store, multi-AZ |
| **Incremental events** during generation | Buffered only | In-process bounded ring, released after a short retention window |
| A complete **event history / transcript** | **No** | Build and keep it yourself from the live stream |

The governing idea is that **storage and subscription are separate concerns**. The
service keeps response items so it can assemble context on demand, but it does not
offer thread-level subscription or open-screen restoration. `GET /v1/responses/{id}`
returns the final response object — not a replayable transcript of how it was
produced.

## Protocol subset

Strictly enforced, and strict in both directions: unknown fields and out-of-subset
types are rejected with `400` rather than ignored. That matches upstream behaviour,
so being strict is simultaneously safer and *more* compatible.

| Layer | Supported | Rejected |
|---|---|---|
| Parameters | `model`, `input`, `instructions`, `store`, `stream`, `background`, `previous_response_id`, `max_output_tokens`, `metadata`, `tools`, `tool_choice`, `temperature`, `top_p` | `conversation`, `context_management`, `prompt` |
| Items | `message`, `function_call`, `function_call_output` | `item_reference`, `reasoning`, `computer_call`, `mcp_*`, hosted tools |
| Content | `input_text`, `output_text`, `refusal`, `input_image`, `input_file` (references only) | inline base64 |

Full contract, error codes and the upstream revision it tracks:
[`docs/design/06-protocol-subset.md`](docs/design/06-protocol-subset.md).

## Quick start

```bash
just verify          # L0 + L1 + L2 (+ L3, which self-skips without a database)
just unittest        # unit tests
just sim             # manual console → http://127.0.0.1:19090  (chat demo: /chat)
```

Multi-turn in two requests — note the second body carries no history:

```bash
curl -sX POST localhost:18080/v1/responses \
  -H 'content-type: application/json' \
  -d '{"model":"m","input":"My name is Ada.","background":true}'
# → {"id":"resp_node-a_…","status":"queued", …}

curl -sX POST localhost:18080/v1/responses \
  -H 'content-type: application/json' \
  -d '{"model":"m","input":"What is my name?","previous_response_id":"resp_node-a_…"}'
```

Streaming and precise resumption:

```bash
curl -N "localhost:18080/v1/responses/$ID?stream=true"
curl -N "localhost:18080/v1/responses/$ID?stream=true&starting_after=7"
```

`starting_after` is **exclusive**. Sequence numbers are 0-based and contiguous, so
`starting_after=N` always means "give me `N+1` onwards". An already-evicted position
returns `410` — no partial data, and deliberately no recovery path.

## Commands

### Verification (CI gates)

| Command | Purpose |
|---|---|
| `just verify` | L0 → L1 → L2 → L3 |
| `just verify l0` | Port contract, run against **every** adapter |
| `just verify l1` | In-process scenarios + Trace / Oracle |
| `just verify l2` | Three peer nodes over HTTP |
| `just verify l3` | End-to-end on PostgreSQL; **skips** when unconfigured |
| `just unittest` | Workspace unit tests |
| `just coverage` | Coverage against the FR/CR/INV baseline |
| `just check-deps` | Layering rules: core depends on no adapter; L0 needs no DB driver |

L0–L2 require no infrastructure at all (D17). L3 needs:

```bash
export NOVA_TEST_DATABASE_URL=postgres://user:pass@127.0.0.1:5432/nova
export NOVA_INTEGRITY_KEY=<at least 16 bytes>
```

### Development fixtures

| Command | Purpose |
|---|---|
| `just sim` | Manual console; the `/chat` page demonstrates the chain (**not** a gate) |
| `just procs up\|down` | Start/stop the three-node fixture |

### Deployment

| Command | Purpose |
|---|---|
| `just deploy …` | Docker Compose; skipped without Docker (D17) |

## Repository layout

```text
.
├── crates/
│   ├── gateway/              # nova-responses-gateway — HTTP ingress
│   ├── core/                 # nova-responses-core — domain, protocol subset, ports
│   └── adapters/
│       ├── mem/              # verification substrate (L0–L2); not durable
│       └── sql/              # PostgreSQL — production carrier and L3
├── testing/
│   ├── conformance/          # L0 port contract, shared by all adapters
│   ├── harness/              # Trace + Oracle + L1/L2/L3 runners
│   ├── mock-agent/           # stand-in execution side
│   ├── sim/                  # manual console
│   ├── scenarios/            # L1 / L2 / L3 YAML
│   ├── config/               # local node topologies
│   └── reports/              # traces and coverage output
├── xtask/                    # gate orchestration
└── docs/                     # requirements · architecture · design · plans
```

## Operational notes

Two rules that are easy to get wrong and expensive to discover late:

1. **Upgrade the gateway before the execution side.** The gateway validates item
   types strictly; if the execution side ships a new type first, the result is a
   spike of `400`s.
2. **Allow the full grace period on restart.** A node stops accepting, drains
   in-flight work, then exits. Cutting this short discards every in-flight
   generation on that node — one impatient deploy costs more than a month of
   unplanned crashes ([D21](docs/architecture/decisions.md#d21-可靠性分层三类存储的差异化投入)).

Secrets (integrity key, database URL, API keys) are read from the environment
only; config files carry variable *names*, never values.

## Documentation

Start at [`docs/README.md`](docs/README.md):
requirements → D20/D21/D22 → [`design/01-responses-api.md`](docs/design/01-responses-api.md).

Current plan: [`docs/plans/current.md`](docs/plans/current.md).
