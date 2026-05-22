# OpenFang Repository Instructions

OpenFang is a Rust workspace for the OpenFang agent runtime, scheduler, API,
CLI, and supporting services. Keep repo-specific claims grounded in current
files and tests.

## Project Contracts

- Treat persisted scheduler state as durable production data. Migrations must be
  idempotent, explicit about what changed, and must not silently skip overdue
  work.
- Schedule semantics belong in scheduler/kernel code and tests, not in UI text
  or operator instructions.
- Long-running scheduled agent work should be split into resumable, bounded
  jobs. Daily jobs should default to shallow/cheap passes unless deep work is
  explicitly scheduled.
- Config additions need the full path: config struct, serde/default handling,
  example config, docs, and tests when behavior changes.
- API additions need route registration, handler implementation, type coverage,
  and at least one test that proves the route is reachable.
- Do not restart live daemons, push, or change shared runtime state unless the
  user explicitly asks for that operation.

## Health Stack

- typecheck: cargo build --workspace --lib
- lint: cargo clippy --workspace --all-targets -- -D warnings
- test: cargo test --workspace
- shell: shellcheck scripts/install.sh
