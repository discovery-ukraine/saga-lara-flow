---
id: configuration
title: Configuration
sidebar_position: 3
---

# Configuration

Every setting lives in `config/saga-lara-flow.php`. This page walks through the sections you are most
likely to touch.

## Database

```php
'database' => [
    'connection' => env('SAGA_LARA_FLOW_DB_CONNECTION'), // null = app default
    'table_prefix' => env('SAGA_LARA_FLOW_TABLE_PREFIX', 'saga_'),
],
```

Point the engine at a dedicated connection to keep its tables separate from your domain data. The
prefix is applied to every table. The package tables always use *this* connection (via
`UsesSagaFlowConnection`), so they are unaffected by tenant DB switching unless you leave the
connection `null`.

## Models

```php
'models' => [
    'flow_run' => Models\FlowRun::class,
    'action_run' => Models\ActionRun::class,
    // flow_event, flow_signal, flow_tag, flow_child, compensation_run, side_effect …
],
```

Swap any model for your own subclass to extend behaviour (casts, relations, scopes).

`FlowRun`'s relations read in the order their rows are meaningful in: `actions()`,
`compensations()`, `sideEffects()` and `children()` by `sequence`, `events()` by `recorded_at` then
`id`, and `signals()` and `tags()` by `id`. An order you append lands *after* that one — call
`reorder()` first to read against it, and also before `chunkById()`, `lazyById()` or `eachById()`,
whose cursor the default order would otherwise outrank.

## Queue

```php
'queue' => [
    'connection' => env('SAGA_LARA_FLOW_QUEUE_CONNECTION'),
    'queue' => env('SAGA_LARA_FLOW_QUEUE', 'default'),
    'after_commit' => env('SAGA_LARA_FLOW_AFTER_COMMIT', true),
    'dispatch_mode' => DispatchMode::Queue,
],
```

Controls where workflow and action jobs run. `after_commit` dispatches jobs only after the
surrounding database transaction commits. Individual runs can override the connection/queue via
`->onConnection()` / `->onQueue()` or the `#[FlowQueue]` attribute.

## Locks

```php
'locks' => [
    'enabled' => true,
    'workflow_ttl_seconds' => 900,
    'action_ttl_seconds' => 900,
    'compensation_ttl_seconds' => 900,
    'block_seconds' => 5,
    'prefix' => 'saga-lara-flow',
],
```

These configure the `WithoutOverlapping` middleware that serializes concurrent drives of a single
run — the idempotency guard. See [Queues, locks & idempotency](./queues-locks-idempotency.md).

## Monitor & repair

`monitor.expiration.defaults` set implicit deadlines (seconds) for `run` / `action` / `signal` —
`null` means no default. `repair.*` configures the doctor pass that recovers runs whose progress was
lost to a dropped job. Both are covered in [Expiration & monitoring](./expiration-and-monitoring.md).

## Reclaim

`actions.reclaim.stale_running` and `sagas.reclaim.stale_running` — off by default — let a `Running`
row be claimed again once it has sat long enough since `started_at` to suspect its worker died. Not
the same thing as a lock TTL. With it off, a worker killed mid-step leaves that step stuck.
[Reclaim & recovery](./reclaim-and-recovery.md) covers the mechanism, how it differs from the other
timing dials, and the full config and per-step override surface.

## Logging

```php
'logging' => [
    'anomaly_level' => env('SAGA_LARA_FLOW_ANOMALY_LOG_LEVEL', 'info'), // null = off
    'channel' => env('SAGA_LARA_FLOW_LOG_CHANNEL'),                     // null = app default
],
```

The engine's second journal, for the things it absorbs silently: a claim lost to whoever already
owned the row, an outcome write rejected because the row changed hands, and a batch already closed
by a duplicate delivery. None of them fails a job, so these lines are the only trace. See
[Reclaim & recovery](./reclaim-and-recovery.md).

## Policies

```php
'children' => ['default_close_policy' => ChildClosePolicy::Abandon],
'parallel' => ['default_failure_policy' => ParallelFailurePolicy::FailFast],
'sagas' => [
    'default_compensation_failure_policy' => CompensationFailurePolicy::Stop,
    'parallel_compensation' => false,
    'compensate_failed_step' => false,
],
```

Defaults for child close behaviour, parallel-block failure handling, and saga compensation. Each can
be overridden per builder call or per attribute — precedence is **action/builder > group > config**.

## Tenancy

```php
'tenancy' => [
    'auto' => false,
    'capture' => null, // fn (): array
    'restore' => null, // fn (array $context): void
    'end' => null,     // fn (?array $previous): void
],
```

Callable hooks for Octane / multi-tenant safety — see [Octane & multi-tenancy](./octane-and-multi-tenancy.md).
