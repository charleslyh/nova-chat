#!/usr/bin/env bash
# 端到端验证：起 mem 后端的能力服务 + 用真实 chat completions 的 agentd + 页面。
#
# 验证结构与能力服务分离：本脚本只**调起**能力服务的二进制（mem-server / sweep /
# agentd / gateway），不修改它们。页面走 gateway 的公开 HTTP API。
#
# 需要（由调用方 export，SEC-4：密钥只走环境变量）：
#   NOVA_CHAT_BASE_URL   chat completions 的 base url（如 https://api.openai.com/v1）
#   NOVA_CHAT_API_KEY    API key
#   NOVA_CHAT_MODEL      可选，固定模型名；不设则用页面里填的 model
set -euo pipefail

cd "$(dirname "$0")/../.."

: "${NOVA_CHAT_BASE_URL:?请先 export NOVA_CHAT_BASE_URL（chat completions 的 base url）}"
: "${NOVA_CHAT_API_KEY:?请先 export NOVA_CHAT_API_KEY}"

export NOVA_MEM_SERVER_URL="${NOVA_MEM_SERVER_URL:-http://127.0.0.1:19000}"
# mem-server 默认校验内容完整性，启动时读 $NOVA_INTEGRITY_KEY（INV-44）；缺失即启动失败。
# 这是验证夹具的本地密钥，仅用于本机验证，绝不出现在部署里（SEC-4）。
export NOVA_INTEGRITY_KEY="${NOVA_INTEGRITY_KEY:-l2-fixture-integrity-key-0123456789}"

# --- 端口工具 ------------------------------------------------------------------

# 等待一个 HTTP 端口就绪（能建立 TCP 连接即可，不关心响应码）。
wait_port() {
  local port=$1 tries=${2:-100}
  for _ in $(seq 1 "$tries"); do
    if curl -s -o /dev/null "http://127.0.0.1:$port/" 2>/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  echo "端口 $port 未就绪" >&2
  return 1
}

# 清理占用验证专用端口的残留进程。只杀命令行含 nova / http.server 的进程，避免误杀
# 用户在这些端口上跑的无关服务；非本项目的占用者只提示、不动手。
free_ports() {
  if ! command -v lsof >/dev/null 2>&1; then
    echo "  （无 lsof，跳过残留清理；冲突将交由 wait_port 超时暴露）"
    return 0
  fi
  local port pid cmd
  for port in "$@"; do
    for pid in $(lsof -nP -iTCP:"$port" -sTCP:LISTEN -t 2>/dev/null || true); do
      cmd=$(ps -p "$pid" -o command= 2>/dev/null || true)
      case "$cmd" in
        *nova*|*http.server*)
          # 变量后紧跟中文全角字符时必须用 ${var} 花括号定界，否则 bash 在 UTF-8
          # locale 下会把「（」等并入变量名，报 unbound variable。
          echo "  清理残留进程 pid=${pid}（占用端口 ${port}）：${cmd:0:60}"
          kill "$pid" 2>/dev/null || true
          ;;
        *)
          echo "  ⚠ 端口 ${port} 被无关进程占用（pid=${pid}）：${cmd:0:60}，跳过清理" >&2
          ;;
      esac
    done
  done
  # 给被 kill 的进程一点时间释放端口，避免紧接的 bind 撞上 TIME_WAIT。
  sleep 0.3
}

# --- 启动 ----------------------------------------------------------------------

echo "==> 编译能力服务（mem 后端）..."
cargo build -q \
  --bin mock-server \
  --bin mock-agentd \
  --bin nova-responses-gateway

echo "==> 清理残留端口（19000 / 19001 / 18080 / 8083）"
free_ports 19000 19001 18080 8083

BIN=target/debug
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT INT TERM

echo "==> 起 mem-server（共享载体，数据面 19000 / 控制面 19001）"
"$BIN/mock-server" --listen 127.0.0.1:19000 --control-listen 127.0.0.1:19001 &
PIDS+=($!)
wait_port 19000

echo "==> 起 agentd（--scheduler http，读 NOVA_CHAT_* 与 NOVA_MEM_SERVER_URL）"
# gateway 内嵌的 sweep 用 2000ms 心跳 TTL，心跳必须远短于它，否则生成稍慢就被
# 中途回收。
"$BIN/mock-agentd" --scheduler http --heartbeat-interval-ms 500 &
PIDS+=($!)

echo "==> 起 gateway（监听 127.0.0.1:18080）"
"$BIN/nova-responses-gateway" --config verify/config/node-a.toml &
PIDS+=($!)
# gateway 启动时会探活 context / conversation / session 三个库，就绪即监听 18080。
wait_port 18080

# 页面固定端口 8083；被占用时 free_ports 已清理，仍冲突则直接报错，不静默换端口
# （否则每次 Ctrl+C 后页面端口会漂移 +1）。
WEB_PORT=8083
if curl -s -o /dev/null "http://127.0.0.1:$WEB_PORT/" 2>/dev/null; then
  echo "❌ 端口 $WEB_PORT 仍被占用（清理失败或为无关进程），请手动处理" >&2
  exit 1
fi

echo
echo "✅ 已就绪"
echo "   gateway API : http://127.0.0.1:18080/v1"
echo "   页面        : http://127.0.0.1:$WEB_PORT"
echo "   模型        : ${NOVA_CHAT_MODEL:-<页面里填写>}"
echo
echo "==> 起页面静态服务（端口 ${WEB_PORT}）"
(cd verify/web && python3 -m http.server "$WEB_PORT" --bind 127.0.0.1) &
PIDS+=($!)

echo "按 Ctrl+C 停止全部。"
wait
