---
id: tags-and-querying
title: Tags & querying
sidebar_position: 13
---

# Tags & querying

## Tagging runs

Attach searchable key/value tags at creation, declaratively, from the parent that starts a child,
from inside the workflow, or from outside through a `FlowHandle`:

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
// on a child, from the parent that starts it
$this->child(ShipmentWorkflow::class, ['order-42'])
    ->withTags(['customer' => $customerId])
    ->run();
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

```php
// from outside, on a loaded handle
SagaFlow::loadFlow($runId)
    ->tag('payment-failed')
    ->withTags(['attempt' => 2, 'orders' => null]);
```

Explicit tags passed to `withTags()` override attribute tags with the same key. On a handle,
`tags()` reads and `withTags()` writes. A child run started with `child()` gets the `#[Tag]`s of
its own class and the tags the parent passes to `withTags()` on the child builder, written together
with the run; it does not get its parent's tags.

Re-tagging an existing key overwrites its value rather than adding a second tag — the database
enforces one row per `(flow_run_id, key)`. Both `tag()` and `tags()` / `withTags()` are idempotent
across replays — safe to call unconditionally at the top of `handle()`.

:::caution Outside tags vs workflow tags
Tags are not history: they carry no sequence and are never consulted during replay. A workflow
calling `$this->tag('x', ...)` in `handle()` re-runs that write on **every replay**, overwriting
whatever a host, or a parent through `child()->withTags()`, set under the same key. Keys written
from outside or by a parent should not collide with keys the workflow writes itself.
:::

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

- `whereTag(string $key, string|int|null $value = null)` — a null `$value` matches any value.
- `whereTagIn(string $key, array $values)` — runs whose tag holds any of these values. Called with
  an empty array, it matches no run. Tag values are stored as strings, and both methods compare an
  `int` as the string it was written as: `7` finds `tag('customer', 7)`, not `'007'`.
- `whereStatus(FlowStatus ...$statuses)` and shortcuts `running()`, `waiting()`, `completed()`, `failed()`
- `active()` (alias `signalable()`) — runs that can still receive a signal: `Pending`, `Running`,
  or `Waiting`. Use this to find a run to deliver a signal to: a flow parked on `awaitSignal()` is
  `Waiting`, not `Running`, so `running()` would miss it.
- `whereWorkflow(string $workflowClass)`
- `whereId(string ...$ids)` — runs with one of these ids. Called with none, it matches no run, so an
  empty selection stays empty.
- `whereAwaitingSignal(?string $name = null)` — runs whose wait for a signal is still open,
  whichever seam opened it (`awaitSignal()` or `retryOnSignal()`). A null `$name` matches any.
- `whereAwaitingRetrySignal(?string $signal = null)` — runs holding a step or a child parked by
  `retryOnSignal()`, i.e. an `action_runs` or a `flow_children` row in `awaiting_retry` whose wait
  is still open or already holds a delivery. A null `$signal` matches any.
- `before(DateTimeInterface)` / `after(DateTimeInterface)` (both filter `created_at`)

```php
// everything blocked on this name, planned waits included
SagaFlow::query()->whereAwaitingSignal('approval')->get();

// only steps that failed and parked
SagaFlow::query()->whereAwaitingRetrySignal('balance-refilled')->handles();

// the runs of a few picked customers
SagaFlow::query()->whereTagIn('customer', $customerIds)->get();

// the parked runs an operator picked, whatever signal each one waits on
SagaFlow::query()->whereAwaitingRetrySignal()->whereId(...$runIds)->handles();
```

### Waits and parked steps

Both seams park the run as `Waiting` and open a signal row, so `flow_signals` alone cannot tell them
apart. What separates them lives on `action_runs`:

| | `awaitSignal('approval')` | `retryOnSignal('balance-refilled')` |
|---|---|---|
| `flow_runs.status` | `waiting` | `waiting` |
| row in `flow_signals` | `approval / waiting` | `balance-refilled / waiting` |
| row in `action_runs` at that wait | none | `awaiting_retry`, `retry_signal` set |
| failure snapshot for operators | none | `exception: {class, message, code}` |

The two also stop matching at different moments. Delivery marks the wait `Received` while the parked
step keeps its `awaiting_retry` status until replay resumes the run. In that gap only
`whereAwaitingRetrySignal()` matches — which is what makes it the filter that finds a run whose
signal arrived but whose resume never did. A timeout marks the wait `TimedOut`, and from then on
neither matches: the next replay gives the retry up, and no signal can change that.

Both read the rows a run holds rather than the run's own status. A run that finishes settles its open
wait and its parked step alike (see [statuses](./statuses.md)), so it drops out of both filters on its
own. Rows left behind by runs that finished before this behaviour existed still carry `waiting` and
`awaiting_retry`, and their runs still match — compose with `signalable()` while any of those remain.

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
