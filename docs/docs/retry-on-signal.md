---
id: retry-on-signal
title: Retry on signal
sidebar_position: 8
---

# Retry on signal

A step that fails hard takes the whole run with it: the saga rolls back and the work that already
succeeded is undone. That is the right default for a bug, but the wrong one for a step that failed
because *the world was not ready yet* — a card declined for insufficient funds, a downstream
service still provisioning, an approval not yet granted.

`retryOnSignal()` parks such a step instead of failing it. The run waits, nothing rolls back, and
when the named signal arrives **only that step runs again**:

```php
public function handle(string $orderId): void
{
    $this->action(CreateOrder::class, $orderId)
        ->compensateWith(CancelOrder::class, $orderId)
        ->run();

    $this->action(ChargeCard::class, $orderId)
        ->compensateWith(RefundCard::class, $orderId)
        ->retryOnSignal('balance-refilled')
        ->run();

    $this->action(ShipOrder::class, $orderId)->run();
}
```

If `ChargeCard` exhausts its `$tries`, the run goes `Waiting` and the step goes `awaiting_retry`.
`CreateOrder` stays completed and un-compensated. Delivering `balance-refilled` re-runs `ChargeCard`
alone; if it succeeds, the workflow continues to `ShipOrder` as if nothing had happened.

## The full signature

```php
->retryOnSignal(
    RetryPolicy|string $signal,
    ?int $maxRetries = null,   // null = unbounded
    ?int $waitSeconds = null,  // null = monitor.expiration.defaults.signal
    ?array $only = null,       // null = park on any exception
    ?Closure $when = null,     // null = park on every failure that passed $only
)
```

- **`$signal`** — the signal name to wait for. An ordinary signal: deliver it exactly the way you
  deliver any other (see [Signals](./signals.md)).
- **`$maxRetries`** — how many signal-gated retry cycles this step may spend. `null` falls back to
  `actions.retry_on_signal.max_retries` in the config, and then to unbounded.
- **`$waitSeconds`** — how long *one* wait may last before the monitor gives up on it. A duration,
  not a moment, because the wait repeats and a `now()->addDay()` would be recomputed on every
  replay.
- **`$only`** — a list of exception classes that may trigger a park. Subclasses count. Anything else
  fails the step normally, so a `TypeError` never parks a saga for a day.
- **`$when`** — the final say on a failure that got past `$only`. It receives a
  [`RetryContext`](#deciding-on-more-than-the-exception-class) and returning `false` ends the policy
  for that failure.

`maxRetries` and `waitSeconds` must be zero or greater — `null`, not a negative number, is how you
say "no limit". A negative value raises an `InvalidArgumentException` rather than reaching the
database, where it would be an error on MySQL and a step that silently never parks elsewhere. The
same applies to the configured `actions.retry_on_signal.max_retries`.

```php
$this->action(ChargeCard::class, $orderId)
    ->compensateWith(RefundCard::class, $orderId)
    ->retryOnSignal(
        'balance-refilled',
        maxRetries: 3,
        waitSeconds: 86400,
        only: [InsufficientBalanceException::class],
    )
    ->run();
```

## Deciding on more than the exception class

`$only` answers one question — *is it one of these classes?* When the decision needs the HTTP status,
the provider's error code, or how much of the budget is already gone, pass a predicate:

```php
use DiscoveryUkraine\SagaLaraFlow\Retry\RetryContext;

->retryOnSignal(
    'balance-refilled',
    maxRetries: 3,
    only: [PaymentFailedException::class],
    // A rejected card is not going to be fixed by topping the balance up.
    when: fn (RetryContext $context): bool => $context->failure->code !== 422,
)
```

The predicate runs during replay, so it must answer the same question the same way on every pass.
Capture workflow arguments, not request state or `now()`.

### A policy you can name and reuse

A closure repeated at five call sites is the same copy-paste the argument list was. Extend
`RetryPolicy` and the whole policy becomes one object with a name, a unit test, and one place to
change:

```php
use DiscoveryUkraine\SagaLaraFlow\Retry\RetryContext;
use DiscoveryUkraine\SagaLaraFlow\Retry\RetryPolicy;

final class BalanceRefillRecovery extends RetryPolicy
{
    public function signal(): string
    {
        return 'balance-refilled';
    }

    public function maxRetries(): ?int
    {
        return 3;
    }

    public function only(): ?array
    {
        return [PaymentFailedException::class];
    }

    public function shouldRetry(RetryContext $context): bool
    {
        return $context->failure->code !== 422;
    }
}
```

```php
$this->action(ChargeCard::class, $orderId)
    ->compensateWith(RefundCard::class, $orderId)
    ->retryOnSignal(new BalanceRefillRecovery)
    ->run();
```

Only `signal()` is required; the other four have defaults that reproduce the plain
`retryOnSignal('name')` behaviour. Construct the policy inside `handle()` — it is rebuilt on every
replay and never stored, so it must not depend on anything a replay cannot reproduce.

Only `shouldRetry()` is guarded. A getter that throws fails the run and leaves that step's
compensation unregistered, the way any throwing expression in `handle()` does.

A policy and the arguments it replaces are two sources of truth for one decision, so combining them
raises an `InvalidArgumentException` at build time.

### What the predicate is given

```php
final readonly class RetryContext
{
    public string $runId;
    public string $workflowClass;
    public string $actionClass;
    public int $sequence;
    public string $signal;
    public int $cyclesSpent;    // signal-gated cycles already spent; 0 the first time
    public ?int $cap;           // the ceiling on the row; null is unbounded
    public int $executions;     // every run of the step, the queue's own retries included
    public RecordedFailure $failure;
}
```

`$failure` is a snapshot of `action_runs.exception` — `class`, `message`, `code`, and
`is(...$classes)` for a subclass-aware check. It is not the thrown object: a queued step fails in
another process, and every later replay reads the row, so there is no stack trace. `class` is `null`
when the row records none, and `is()` answers `false` for such a failure.

:::warning It decides whether to park, not whether to wake
Once a step is parked, the signal that arrives spends a cycle and re-runs it. The predicate is not
asked again. A policy that changes its mind mid-wait does not cancel a wait already advertised —
cancel the run, or let the wait time out.
:::

:::info One value is frozen; no method is
All five methods run on every replay — the builder rebuilds the policy each pass, so even a step
already scheduled or completed calls them again.

`maxRetries()` is the one whose value is kept: it is written to
`action_runs.retry_signal_max_attempts` when the step is scheduled, and every later replay enforces
the row, so raising it in a deploy does not lift the ceiling of a step already parked. See
[the budget is read from the row](#the-budget-is-read-from-the-row-not-from-your-code).

The other four take effect immediately, for runs already in flight included.
`action_runs.retry_signal` records the name a step last parked on but is rewritten at every parking
and never read back, so renaming a signal moves what a parked step will next wait for and abandons a
delivery already made under the old name. That is true of the plain string form too.
:::

A predicate may not write to the run it is deciding for — no `action()`, `awaitSignal()`, `child()`,
`sideEffect()` or `tag()`, and no `signal()`, `cancel()`, `compensate()` or `withTags()` on that
run's own handle.
It is not asked again once the step it guards succeeds, so an ordinal it consumed would be left
unclaimed and the next step would land in the wrong slot, and a run it cancelled would be handed a
live wait a moment later. The engine refuses the attempt with `RetryPolicyReentryException` before
anything is written. Other runs are fair game.

A predicate that throws anything else is read as `false`: the step fails as it would have without a
policy, and the run carries the step's own business failure rather than the policy's. The throw is
recorded in the anomaly log under `retry_policy_threw`, because a policy that quietly throws on
every call looks exactly like one that never parks anything.

The predicate is not called exactly once — a process that dies between the decision and the parking
makes the next replay ask again. Keep it pure and put side effects in a listener.

## Where it sits among the other failure layers

Failure handling stacks, and `retryOnSignal()` sits in the middle:

1. **Laravel's queue retries** (`public int $tries` on the action) run first. The step parks only
   once the queue has given up on it.
2. **`retryOnSignal()`** parks and waits. Every arriving signal spends one cycle and re-runs the
   step, which starts its `$tries` over.
3. **`continueOnFailure()`** — see [Optional actions](./optional-actions.md) — takes over only after
   the retry budget is spent, the wait times out, or the policy refuses the failure. An optional step
   with a retry policy waits first and falls back to its fallback value second.
4. **Hard failure and compensation** — the ordinary path from
   [Sagas & compensations](./sagas-and-compensation.md).

When the policy gives up, the step fails exactly as it would have without any retry policy: the same
`ActionFailedException`, carrying the message of the **last** attempt, and the same rollback. The
seam is transparent on the way out — there is no new exception class to catch.

## What ends the waiting

A step stops waiting when any one of these happens:

- **The signal arrives.** One cycle is spent, the step runs again.
- **The budget runs out.** `retry_signal_attempts` reaches `maxRetries` → hard failure.
- **The wait times out.** The monitor flips the wait to `timed_out` → hard failure.
- **The policy refuses the failure.** `only` does not list it, or `shouldRetry()` / `when` returns
  `false` → hard failure.
- **The run expires** on its own `expires_at`, or is cancelled. The park ends with it: the step is
  settled as `cancelled` and its wait along with it, so nothing keeps advertising a signal that
  would no longer be acted on. See [statuses](./statuses.md).

:::info A timed-out wait ends the policy for good
The wait deadline bounds *the waiting*, not one wait out of several. Once a wait has timed out, the
step fails even if budget is left — otherwise a signal that is never coming would hand the step a
fresh deadline on every replay and the run would never finish.
:::

## The budget is read from the row, not from your code

A step's ceiling is written to `action_runs.retry_signal_max_attempts` when the step is scheduled,
and every later replay reads it from there. Changing `maxRetries:` in the workflow — or the config
default — does **not** move the ceiling under a run that is already parked. Runs started after the
deploy get the new value.

That is deliberate: the budget belongs to the run, and a redeploy must not silently extend or cut
short a wait that is already in flight.

## Inside a saga group

`saga()->step()` mirrors the method, and it means exactly the same thing:

```php
$this->saga()
    ->step(CreateOrder::class, $orderId)->compensateWith(CancelOrder::class, $orderId)
    ->step(ChargeCard::class, $orderId)
        ->compensateWith(RefundCard::class, $orderId)
        ->retryOnSignal(new BalanceRefillRecovery)
    ->step(ShipOrder::class, $orderId)
    ->run();
```

## Delivering the signal

Nothing special — it is an ordinary signal:

```php
SagaFlow::loadFlow($runId)->signal('balance-refilled');
```

```bash
php artisan saga-flow:signal 01JABCDEF... balance-refilled
```

Usually you do not have the run id at hand. Query for it with `whereAwaitingRetrySignal()`, and
filter with `signalable()` (a parked run is `Waiting`, never `Running`):

```php
SagaFlow::query()
    ->whereWorkflow(CheckoutWorkflow::class)
    ->whereTag('customer', $customerId)
    ->whereAwaitingRetrySignal('balance-refilled')
    ->signalable()
    ->handles()
    ->first()
    ?->signal('balance-refilled');
```

`whereAwaitingSignal()` is the wider filter: it also matches a run parked by `awaitSignal()` on that
name. See [Tags & querying](./tags-and-querying.md#waits-and-parked-steps).

The signal's **payload is not passed to the action**. Action arguments must stay identical across
replays, so the retried step runs with the arguments it was given originally; the payload is stored
on the signal row for auditing. If the retry needs new data, read it inside the action.

## Observing parked runs

`saga-flow:list` annotates a parked run with the signal it is waiting for, and `saga-flow:show`
gains a **Retry** column showing the signal, the spent budget, and the current deadline:

```
Seq  Status          Action       Attempts  Retry                                  Finished
1    completed       CreateOrder  1         —                                      ...
2    awaiting_retry  ChargeCard   3         balance-refilled 1/3 until 2026-08-24…  ...
3    pending         ShipOrder    0         —
```

Two events cover the lifecycle — see [Events](./events.md):

- **`ActionAwaitingRetry`** — fires once per park, carrying the step and the signal name.
- **`ActionRetried`** — fires once per cycle, when the step is about to run again.

Both are dispatched after the surrounding transaction commits, so a listener never reacts to a retry
the database rolled back.

## Determinism

A retry does **not** consume a new sequence. The step keeps its `(flow_run_id, sequence)` ordinal
and reuses the same `action_runs` row for every cycle, and waiting consumes no ordinal at all. That
means downstream steps land on identical ordinals whether the step retried zero times or ten, and
`handle()` replays exactly as it did before — see [Determinism rules](./determinism-rules.md).

The counters are two different things: `attempts` keeps counting *every* execution cumulatively
(including queue retries), while `retry_signal_attempts` counts only the signal-gated cycles.

## Things worth knowing

- **Don't wrap a step in your own `DB::transaction()`.** The engine suspends by throwing, so an
  application transaction around `->run()` would roll the suspension's bookkeeping back. This is true
  of every suspending seam in the package, not only this one.
- **Package event listeners must be queued (`ShouldQueue`) or must not throw.** A throwing listener
  on `ActionRetried` or `FlowSignalConsumed` interrupts the replay mid-way, and the engine reads that
  as a business failure.
- **Turn `repair.enabled` on in production.** If a process dies between committing a retry and
  dispatching its job, the step sits `Pending` with no job behind it. The doctor re-dispatches it —
  but it is off by default. See [Expiration & monitoring](./expiration-and-monitoring.md).
- **A signal delivered in the same second the step failed counts for that failure.** The engine
  matches a floating signal to the attempt with second resolution, so a signal that landed just
  *before* the failure in the same second is treated as arriving after it. The cost is bounded: at
  most one extra cycle, and the budget still applies.
- **The three wait-signal transitions raise no Eloquent model events.** Delivery into an open wait,
  closing a superseded wait, and timing a wait out are written as single conditional `UPDATE`s —
  the only form that is atomic on every supported driver. An observer registered on a swapped-in
  `models.flow_signal` will not see them. Every transition is still recorded in `flow_events`, and
  where one exists, in the package's own Laravel event.
