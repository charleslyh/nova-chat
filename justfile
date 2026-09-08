# Verification gates (automated; `sim` is manual and deliberately excluded).
#
#   just verify        → L0 + L1 + L2 (no infrastructure required)
#   just verify l0     → port contract only
#
# Secrets are read from the environment only — never from config files (SEC-4).
[group('verify')]
verify level="all":
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "{{level}}" = "all" ]; then
      just verify l0
      just verify l1
      just verify l2
      # Self-skipping: a machine without python3/openai is not at fault.
      just verify l4
    else
      cargo run -q -p xtask -- verify --level "{{level}}"
    fi

[group('verify')]
unittest:
    cargo run -q -p xtask -- unittest

[group('verify')]
coverage:
    cargo run -q -p xtask -- coverage

# Guards the layering rules: core depends on no adapter, and L0 builds without a
# database driver.
[group('verify')]
check-deps:
    cargo run -q -p xtask -- check-deps

# Local process fixture: three peer nodes (18080/18081/18082) plus a mock agent.
# Fixture credentials are injected as environment variables by xtask.
[group('dev')]
procs cmd:
    cargo run -q -p xtask -- procs {{cmd}}

# Manual console at http://127.0.0.1:19090 — drives a real multi-turn chain so the
# `previous_response_id` flow is observable end to end.
[group('dev')]
sim:
    cargo run -p sim -- --listen 127.0.0.1:19090

default:
    @just --list
