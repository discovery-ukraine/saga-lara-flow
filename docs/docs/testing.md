---
id: testing
title: Testing your workflows
sidebar_position: 21
---

# Testing your workflows

## Synchronous assertions

For a workflow that doesn't need to suspend, `runSync()` drives every step in-process and lets you
assert the final state directly:

```php
$run = SagaFlow::create(CheckoutWorkflow::class)
    ->withArguments('order-1')
    ->runSync();

expect($run->status)->toBe(FlowStatus::Completed)
    ->and($run->result)->toBe(['charge' => 'ch_order-1']);
```

## Queued paths need a real queue

The **queued** paths (suspension, resume, queued actions, parallel blocks) must run against a real
database queue driven with `queue:work --stop-when-empty`. The `sync` driver bypasses the
suspend/replay machinery and will not exercise the engine faithfully.

A typical pattern: set the queue connection to `database`, dispatch with `->run()`, then drain the
queue before asserting.

```php
config()->set('queue.default', 'database');

$run = SagaFlow::create(CheckoutWorkflow::class)->withArguments('order-1')->run();

Artisan::call('queue:work', ['--stop-when-empty' => true]);

expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Completed);
```

The package's own suite (`tests/`) is a working reference — see `tests/Helpers.php` for the
`useDatabaseQueue()` / `drainQueue()` helpers it uses to drive queued flows deterministically.

## Ageing timestamps

To test expiration/repair without waiting, age a row's timestamps directly rather than sleeping:

```php
FlowRun::query()->whereKey($run->id)->update([
    'created_at' => now()->subDay(),
    'updated_at' => now()->subDay(),
]);
```

## Running the package's own tests

```bash
composer test        # Pest
composer analyse     # PHPStan (larastan, level 5)
composer lint        # Pint + PHPStan
```

The suite runs with random order and fails on risky/warning-producing tests, so keep tests
isolated and deterministic.
