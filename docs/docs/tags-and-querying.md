---
id: tags-and-querying
title: Tags & querying
sidebar_position: 12
---

# Tags & querying

## Tagging runs

Attach searchable key/value tags at creation, declaratively, or from inside the workflow:

```php
SagaFlow::create(CheckoutWorkflow::class)
    ->withTags(['tenant' => 'acme', 'channel' => 'web'])
    ->run();
```

```php
// declaratively on the workflow class (repeatable)
#[Tag('orders')]
#[Tag('team', 'checkout')]
class CheckoutWorkflow extends Workflow { /* ... */ }
```

```php
// from inside handle()
$this->tag('priority', 'high');

// or several at once
$this->tags([
    'priority' => 'high',
    'attempt' => 2,      // int values are cast to string
    'orders' => null,    // a tag with no value
]);
```

Explicit tags passed to `withTags()` override attribute tags with the same key.

Both `tag()` and `tags()` return the workflow, so they chain:

```php
$this->tag('priority', 'high')->tags(['tenant' => 'acme']);
```

Re-tagging an existing key overwrites its value rather than adding a second tag, and both methods
are idempotent across replays — safe to call unconditionally at the top of `handle()`.

## Querying runs

`SagaFlow::query()` returns a fluent, type-safe `FlowQuery` over flow runs:

```php
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;

$stuck = SagaFlow::query()
    ->whereWorkflow(CheckoutWorkflow::class)
    ->whereTag('tenant', 'acme')
    ->waiting()
    ->before(now()->subHour())
    ->get(); // Collection<FlowRun>
```

### Filters

- `whereTag(string $key, ?string $value = null)`
- `whereStatus(FlowStatus ...$statuses)` and shortcuts `running()`, `waiting()`, `completed()`, `failed()`
- `active()` (alias `signalable()`) — runs that can still receive a signal: `Pending`, `Running`,
  or `Waiting`. Use this to find a run to deliver a signal to: a flow parked on `awaitSignal()` is
  `Waiting`, not `Running`, so `running()` would miss it.
- `whereWorkflow(string $workflowClass)`
- `before(DateTimeInterface)` / `after(DateTimeInterface)` (both filter `created_at`)

### Terminals

- `get(): Collection<FlowRun>`
- `first(): ?FlowRun`
- `count(): int`
- `paginate(int $perPage = 15): LengthAwarePaginator`
- `handles(): Collection<FlowHandle>` — hydrate matched runs as operable handles
- `builder(): Builder<FlowRun>` — escape hatch to the raw Eloquent builder for ordering/limits

```php
$handles = SagaFlow::query()->running()->handles();
$page    = SagaFlow::query()->failed()->paginate(25);
$latest  = SagaFlow::query()->builder()->latest()->limit(10)->get();
```
