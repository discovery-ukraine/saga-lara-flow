---
id: your-first-workflow
title: Your first workflow
sidebar_position: 4
---

# Your first workflow

Generate a workflow and an action with the bundled generators:

```bash
php artisan make:workflow ProvisionAccountWorkflow
php artisan make:action  CreateTenant
```

## The workflow

A **workflow** extends `Workflow` and implements a deterministic `handle()`. You never `new` an
action — you schedule it through the DSL, and the engine runs, records, and replays it:

```php
use DiscoveryUkraine\SagaLaraFlow\Workflow;

class ProvisionAccountWorkflow extends Workflow
{
    public function handle(string $email): array
    {
        $tenantId = $this->action(CreateTenant::class, $email)->run();
        $this->action(SendWelcomeEmail::class, $email)->run();

        return ['tenant' => $tenantId];
    }
}
```

Arguments passed to `withArguments()` at creation are forwarded, in order, to `handle()`.

## The action

An **action** extends `Action` and does the actual work, with native Laravel dependency injection in
the constructor and/or `handle()`:

```php
use DiscoveryUkraine\SagaLaraFlow\Action;

class CreateTenant extends Action
{
    public function handle(TenantRepository $tenants, string $email): string
    {
        return $tenants->provision($email)->id;
    }
}
```

The arguments you pass to `$this->action(CreateTenant::class, $email)` are appended after the
injected dependencies.

## Starting it

```php
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;

// Queued: returns a Pending FlowRun immediately, drives on your workers.
$run = SagaFlow::create(ProvisionAccountWorkflow::class)
    ->withArguments('jane@example.com')
    ->run();

// Synchronous: drives every step in-process, returns a Completed FlowRun.
$run = SagaFlow::create(ProvisionAccountWorkflow::class)
    ->withArguments('jane@example.com')
    ->runSync();
```

The `create()` builder also exposes `->onConnection()`, `->onQueue()`, `->withTags()`,
`->version()`, and `->expiresAt()`. From here, explore [Actions](./actions.md) and
[Sagas & compensations](./sagas-and-compensation.md).
