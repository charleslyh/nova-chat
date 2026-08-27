# 验证 / 测试（自动门禁；sim 不在此列）
# just verify       → L0+L1+L2
# just verify l0    → 单层
[group('verify')]
verify level="all":
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "{{level}}" = "all" ]; then
      just verify l0
      just verify l1
      just verify l2
    else
      cargo run -q -p xtask -- verify --level "{{level}}"
    fi

[group('verify')]
unittest:
    cargo run -q -p xtask -- unittest

[group('verify')]
coverage:
    cargo run -q -p xtask -- coverage

[group('verify')]
check-deps:
    cargo run -q -p xtask -- check-deps

# 本机进程夹具
[group('dev')]
procs cmd:
    cargo run -q -p xtask -- procs {{cmd}}

[group('dev')]
sim:
    cargo run -p sim -- --listen 127.0.0.1:19090

# 部署（可选 Docker）
[group('deploy')]
deploy cmd:
    cargo run -q -p xtask -- deploy {{cmd}}

default:
    @just --list
