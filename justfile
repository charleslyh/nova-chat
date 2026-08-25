default:
    @just --list

verify level:
    cargo run -q -p xtask -- verify --level {{level}}

procs cmd:
    cargo run -q -p xtask -- procs {{cmd}}

coverage:
    cargo run -q -p xtask -- coverage

check-deps:
    cargo run -q -p xtask -- check-deps

deploy cmd:
    cargo run -q -p xtask -- deploy {{cmd}}

sim:
    cargo run -p nova-sim -- --listen 127.0.0.1:19090

test:
    cargo test --workspace --exclude nova-server --exclude nova-mock-worker --exclude xtask --exclude nova-sim
