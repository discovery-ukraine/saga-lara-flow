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
