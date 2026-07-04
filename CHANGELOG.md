# Changelog

All notable changes to `saga-lara-flow` will be documented in this file.

## v1.0.4 - 2026-07-04

### Documentation

- Clarified how to react to a failed step. `try/catch` around `->run()` inside `handle()` **does**
  catch `ActionFailedException` / `FlowExpiredException` (and `ChildWorkflowFailedException`) — but on
  the **replay** pass, not the instant the action fails: in queued mode the action runs off the
  `handle()` stack in its own job, and the failure surfaces only when the engine re-drives `handle()`.
  In sync mode `run()` re-throws the action's raw exception instead. Documented that `try/catch` is
  for **local** branching, while the `FlowFailed` event is the recommended hook for cross-cutting
  failure reporting (it fires once on the terminal transition, on both the direct-fail and
  fail-after-compensation paths, sync or queued), and that reporting from inside `handle()` must
  re-throw so the run still fails and compensates. Touches the Actions, Child workflows, and Events
  docs plus the README. No code changes.

## v1.0.3 - 2026-07-02

### Fixed

- The migration now ships with a timestamp-prefixed filename
  (`2026_07_02_000000_create_saga_lara_flow_initial_tables`), matching how first-party Laravel
  packages (Sanctum, Cashier, Telescope) name their auto-loaded migrations. In 1.0.2 it loaded under
  a bare, dateless name (`create_saga_lara_flow_initial_tables`), which looked wrong in
  `migrate:status` and sorted unpredictably against the app's own migrations.

### Upgrading from 1.0.1 / 1.0.2

The migration's recorded name changes, so a host app that already ran 1.0.1 or 1.0.2 will see the
new name as "pending" and `php artisan migrate` would try to create the tables again. The engine
tables already exist, so either:

- **Fresh dev database:** `php artisan migrate:fresh` (destroys data — dev only), or
- **Keep your data:** rename the recorded migration so Laravel treats it as already run —
  `UPDATE migrations SET migration = '2026_07_02_000000_create_saga_lara_flow_initial_tables' WHERE migration = 'create_saga_lara_flow_initial_tables';`

Brand-new installs (no prior 1.0.1/1.0.2) are unaffected — `composer require` + `php artisan migrate`.

## v1.0.2 - 2026-07-02

### Fixed

- **`runsMigrations()` did not actually load the migration in 1.0.1.** The migration shipped as a
  `.php.stub`, but Laravel's migrator only treats a registered path as a migration file when it ends
  in `.php` (`Migrator::getMigrationFiles`); a `.php.stub` path was globbed as if it were a directory,
  matched nothing, and was **silently skipped** — so `php artisan migrate` (and `migrate:status`)
  never saw the engine's tables. The migration is now a real `.php` file and runs as intended with a
  plain `php artisan migrate`, no publish step.

### Changed

- Publishing the migration is **no longer supported** as a customization path: because the migration
  auto-runs from the package, a published (timestamped) copy would run alongside it and collide.
  Customize the schema through config instead — `database.table_prefix`, `database.connection`, and
  the swappable `models.*`.

## v1.0.1 - 2026-07-01

### Changed

- The engine's migration now loads directly from the package (`runsMigrations()`), so a host app
  installs with just `php artisan migrate` — the `vendor:publish --tag="saga-lara-flow-migrations"`
  step is no longer required. Future package migrations are picked up the same way, via
  `composer update` + `php artisan migrate`. Publishing the migration is still supported for anyone
  who wants to customize the schema (a published copy overrides the package's own).

## v1.0.0 - 2026-07-01

**Saga Lara Flow 1.0.0** — the first stable release.

A workflow management engine with an integrated **Saga pattern**, built on top of Laravel Queues.
Write a long-running, durable business process as a single deterministic PHP method: each step runs,
is recorded, and survives worker restarts through exception-based suspension and replay. When a step
fails partway through, registered compensations roll back the completed work in reverse order.

### Highlights

- **Deterministic workflows** via exception-based suspension and replay, keyed by `(flow_run_id, sequence)`.
- **Actions** with native Laravel DI, retries (`$tries`), timeouts (`$timeout`), and per-step deadlines.
- **Saga pattern**: action-level and grouped compensations with configurable failure policies, sequential or parallel
  rollback.
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
