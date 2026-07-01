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

You can also deliver from the CLI — see [Artisan commands](./artisan-commands.md):

```bash
php artisan saga-flow:signal {run} approval --payload='{"approved":true}'
```
