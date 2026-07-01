# Changelog

All notable changes to `saga-lara-flow` will be documented in this file.

## 1.0.0 - Unreleased

Initial release.

- Deterministic workflows via exception-based suspension and replay, keyed by `(flow_run_id, sequence)`.
- Actions with native Laravel DI, retries (`$tries`), timeouts (`$timeout`), and per-step deadlines.
- Saga pattern: action-level and grouped compensations with configurable failure policies, sequential
  or parallel rollback.
- Signals with optional timeouts; side effects with record-and-replay.
- Parallel action blocks (`failFast` / `waitAllThenFail`) and optional actions with fallbacks.
- Child workflows with close policies (`Abandon` / `Cancel` / `Fail`).
- Tags and a fluent, type-safe `FlowQuery`.
- Expiration monitoring (scheduler or queue-looping) and a repair/doctor pass for dropped jobs.
- Octane / multi-tenancy safety via capture-at-creation and opt-in auto restore/revert hooks.
- Configurable attributes (`#[Flow]`, `#[FlowQueue]`, `#[FlowTimeout]`, `#[Tag]`, `#[Tenancy]`, …).
- Artisan tooling: `saga-flow:list|show|signal|cancel|kick|monitor|repair|prune`, `make:workflow`,
  `make:action`.
