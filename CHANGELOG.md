# Changelog

All notable changes to `saga-lara-flow` will be documented in this file.

## v1.0.0 - 2026-07-01

**Saga Lara Flow 1.0.0** — the first stable release.

A workflow management engine with an integrated **Saga pattern**, built on top of Laravel Queues.
Write a long-running, durable business process as a single deterministic PHP method: each step runs,
is recorded, and survives worker restarts through exception-based suspension and replay. When a step
fails partway through, registered compensations roll back the completed work in reverse order.

### Highlights

- **Deterministic workflows** via exception-based suspension and replay, keyed by `(flow_run_id, sequence)`.
- **Actions** with native Laravel DI, retries (`$tries`), timeouts (`$timeout`), and per-step deadlines.
- **Saga pattern**: action-level and grouped compensations with configurable failure policies, sequential or parallel rollback.
- **Signals** with optional timeouts; **side effects** with record-and-replay.
- **Parallel action blocks** (`failFast` / `waitAllThenFail`) and **optional actions** with fallbacks.
- **Child workflows** with close policies (`Abandon` / `Cancel` / `Fail`).
- **Tags** and a fluent, type-safe `FlowQuery`.
- **Expiration monitoring** (scheduler or queue-looping) and a **repair/doctor** pass for dropped jobs.
- **Octane / multi-tenancy** safety via capture-at-creation and opt-in auto restore/revert hooks.
- **Configurable attributes** (`#[Flow]`, `#[FlowQueue]`, `#[FlowTimeout]`, `#[Tag]`, `#[Tenancy]`, …).
- **Artisan tooling**: `saga-flow:list|show|signal|cancel|kick|monitor|repair|prune`, `make:workflow`, `make:action`.

### Requirements

- PHP `^8.5`
- Laravel 13 (`illuminate/*: ^13`)

### Install

```bash
composer require discovery-ukraine/saga-lara-flow
php artisan vendor:publish --tag="saga-lara-flow-migrations"
php artisan migrate

```
### Links

- Documentation: https://sagalaraflow.dev
- Packagist: https://packagist.org/packages/discovery-ukraine/saga-lara-flow

**Full Changelog**: https://github.com/discovery-ukraine/saga-lara-flow/commits/v1.0.0

## 1.0.0 - 2026-07-01

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
