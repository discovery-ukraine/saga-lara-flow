---
id: octane-and-multi-tenancy
title: Octane & multi-tenancy
sidebar_position: 18
---

# Octane & multi-tenancy

The engine runs each workflow/action `handle()` in the tenant the run was **created** for, and
reverts afterwards, so nothing leaks between runs on a shared Octane or queue worker.

## How it works

- **Capture at creation.** `SagaFlow::create(...)` snapshots the current tenant via the
  `tenancy.capture` hook and stores it on `flow_runs.tenancy_context` (JSON). Child runs inherit the
  parent's context. Capture is unconditional — it feeds both auto-restore and manual discovery. With
  no `capture` hook it stores `null`.
- **Boundaries.** Every place user code runs is wrapped by the tenancy manager: the workflow
  `handle()`, action `handle()` (including queued action jobs), compensation jobs, and child-cancel
  jobs.
- **Auto-restore is opt-in.** Off by default (`tenancy.auto`). When on, the boundary calls the
  `tenancy.restore` hook before `handle()` and reverts after.
- **Leak guard (revert).** Before restoring, the boundary captures the previous context; in a
  `finally` it reverts — via the optional `tenancy.end` hook when set, otherwise by restoring the
  previous context. So after any boundary the ambient tenant is exactly what it was before.

## Config hooks

```php
// config/saga-lara-flow.php
'tenancy' => [
    'auto'    => false,                                    // opt into auto restore/revert
    'capture' => fn (): array => ['tenant' => tenant()?->getTenantKey()],
    'restore' => fn (array $c): void => tenancy()->initialize($c['tenant']),
    'end'     => null,                                     // optional explicit revert (else restore-previous)
],
```

## Per-class override

`#[Tenancy(auto: true|false)]` on a workflow or action wins over the config default (precedence:
**attribute > config**). Turn auto on for one workflow while keeping the global default off, or opt a
specific step out for manual control.

```php
#[Tenancy(auto: true)]
class ProvisionAccountWorkflow extends Workflow { /* ... */ }
```

## Manual control / discovery

Even with auto off, the run's tenant is available inside `handle()` without threading it through
arguments:

```php
$context = SagaFlow::tenancyContext(); // ['tenant' => '…'] or null
```

Use it to `tenancy()->initialize(...)` / `->end()` yourself when auto-capture doesn't fit — for
example, to open and close the context around only part of a step.

## Host integration example (stancl/tenancy)

```php
'capture' => fn () => tenant() ? ['tenant' => tenant()->getTenantKey()] : ['tenant' => null],
'restore' => function (array $c): void {
    $c['tenant'] === null
        ? tenancy()->end()
        : tenancy()->initialize($c['tenant']);
},
```

:::note
The package's own tables use its configured connection
(`config('saga-lara-flow.database.connection')`), so they are unaffected by tenant DB switching
unless that connection is left `null`.
:::
