---
id: signals
title: Signals
sidebar_position: 7
---

# Signals

Signals let external code push data or decisions into a running workflow. Inside `handle()`,
`awaitSignal()` suspends the workflow until the named signal arrives, then returns its payload:

```php
public function handle(): void
{
    $decision = $this->awaitSignal('approval'); // suspends until delivered

    if (($decision['approved'] ?? false) === true) {
        $this->action(Publish::class)->run();
    }
}
```

A signal delivered *before* the workflow awaits it is consumed inline without suspending.

## Timeouts

The fluent form adds a deadline that turns an unanswered wait into a catchable exception:

```php
use DiscoveryUkraine\SagaLaraFlow\Exceptions\AwaitSignalTimeoutException;

try {
    $decision = $this->signal('approval')
        ->timeoutAfter(now()->addDay())
        ->wait();
} catch (AwaitSignalTimeoutException $e) {
    $this->action(AutoReject::class)->run();
}
```

`awaitSignal($name, $timeout)` accepts the timeout as an optional second argument as well.

:::warning A deadline does not enforce itself

The package has no durable timers. A deadline is a value stored on the wait; something has to
*notice* that it passed. That something is the expiration sweep — either the scheduled
`saga-flow:monitor` command or the opt-in queue-looping listener. Until a sweep runs, the wait stays
open and `AwaitSignalTimeoutException` is never thrown, however long the deadline has been past.

If you set no deadline and no `monitor.expiration.defaults.signal`, the wait is unbounded by design.

See [Expiration & monitoring](./expiration-and-monitoring.md) for how to drive the sweep, and
[Testing](./testing.md#testing-deadlines) for driving it from a test.
:::

## Delivering a signal

From anywhere in your app, deliver via the flow handle:

```php
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;

SagaFlow::loadFlow($runId)->signal('approval', ['approved' => true]);
```

`signal()` throws `CannotSignalTerminalFlowException` if the run has already finished. Use the safe
variant to no-op instead:

```php
$delivered = SagaFlow::loadFlow($runId)->signalIfRunning('approval', ['approved' => true]);
// $delivered === false on a terminal run
```

`signalIfRunning()` means *"unless the run has already finished"* — it delivers to **any
non-terminal run**, not only a `Running` one.

Neither may be called inside a `DB::transaction()` of your own: a rollback afterwards takes the
delivery with it, the wait stays open, and nothing tells you. See
[what a host transaction leaves behind](./queues-locks-idempotency.md#host-transactions).

### Finding the run to signal

Often you do not have the `$runId` on hand — you know the workflow and a tag. Query for it:

```php
SagaFlow::query()
    ->whereWorkflow(ProvisionCompanyWorkflow::class)
    ->whereTag('company', $companyId)
    ->signalable()            // Pending, Running, or Waiting — NOT running()
    ->handles()
    ->first()
    ?->signal('owner-synced');
```

Use `signalable()` (alias `active()`), **not** `running()`. A signal is accepted by any
non-terminal run — `Pending`, `Running`, or `Waiting` — and a flow parked on `awaitSignal()` sits in
**`Waiting`**, not `Running`. Filtering by `running()` would silently miss exactly the run you are
trying to wake.

## Reviving a failed step

A signal can also restart a step that already failed, instead of being awaited at a point in
`handle()`. `->retryOnSignal('balance-refilled')` on an action parks it when it fails and re-runs it
when that signal is delivered — using the same delivery API as everything above. See
[Retry on signal](./retry-on-signal.md).

You can also deliver from the CLI — see [Artisan commands](./artisan-commands.md):

```bash
php artisan saga-flow:signal {run} approval --payload='{"approved":true}'
```
