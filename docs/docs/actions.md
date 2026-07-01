---
id: actions
title: Actions
sidebar_position: 5
---

# Actions

`action(string $actionClass, mixed ...$arguments)` returns an `ActionBuilder`; `run()` executes the
action and returns its result. Arguments passed to `action()` are forwarded to the action's
`handle()` after its injected dependencies.

```php
$tenantId = $this->action(CreateTenant::class, $email)->run();
```

## Retries and timeouts

Retries and per-attempt timeouts use **native Laravel queue semantics** — declare them as public
properties on the action:

```php
use DiscoveryUkraine\SagaLaraFlow\Action;

class ChargeCard extends Action
{
    public int $tries = 3;    // up to 3 attempts when queued
    public int $timeout = 30; // seconds per attempt (0 = none)

    public function handle(PaymentGateway $gateway, string $orderId): string
    {
        return $gateway->charge($orderId);
    }
}
```

You can also set these declaratively with `#[ActionTimeout(seconds: 30)]` (and name a step with
`#[ActionName('charge-card')]`).

## Per-step deadline

Independent of the queue timeout, a step can carry a **deadline** after which it expires:

```php
$this->action(ChargeCard::class, $orderId)
    ->expiresAt(now()->addMinutes(2))
    ->run();
```

There is no `timeoutAfter()` on an action builder — that method belongs to
[signals](./signals.md). Action deadlines use `expiresAt()`.

## Handling failure

`run()` throws when the action ultimately fails (after exhausting `$tries`), so the workflow can
react instead of dying:

```php
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowExpiredException;

try {
    $this->action(ChargeCard::class, $orderId)->run();
} catch (ActionFailedException $e) {
    // retries exhausted
} catch (FlowExpiredException $e) {
    // the step or run passed its deadline
}
```

:::warning Never catch control flow
Do not catch `DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FlowSuspended` (or any
`InternalFlowControl`) — those are the engine's suspend/replay signals, not errors. If you use a
broad `catch (\Throwable $e)`, re-throw control flow first:

```php
} catch (\Throwable $e) {
    if ($this->isFlowControl($e)) {
        throw $e;
    }
    // handle real errors here
}
```
:::

To make a failing action *not* fail the flow, see [Optional actions](./optional-actions.md). To undo
completed work when a later step fails, see [Sagas & compensations](./sagas-and-compensation.md).
