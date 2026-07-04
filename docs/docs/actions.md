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

### When (and where) it throws

The failure does **not** surface at the moment the action fails — it surfaces on the next replay:

- **Queued mode.** The action runs in its own `RunActionJob`, *off* the `handle()` stack, and retries
  per its `$tries`. Only once it has ultimately failed does the engine re-drive `handle()` from the
  top; the recorded-`Failed` step then replays as a throw, and *that* is the moment your `try/catch`
  around `->run()` catches `ActionFailedException`. So a `try/catch` in `handle()` genuinely does
  catch the failure — just on the replay pass, which is the whole point of deterministic replay.
- **Sync mode.** The step runs inline and `run()` re-throws the action's **raw** exception (not
  `ActionFailedException`). Catch the concrete exception type your action can throw, or the base
  `DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowException` plus your own types.

:::tip Local branching vs. cross-cutting reporting
`try/catch` around `->run()` is for **local** decisions — "if `ChargeCard` fails, try PayPal
instead". For a cross-cutting "report whenever *any* workflow fails", listen to the
[`FlowFailed`](./events.md) event instead: it fires once on the terminal transition, on both the
direct-fail and the fail-after-compensation paths, independent of sync/queued. If you *do* report
from inside `handle()`, **re-throw** so the engine still fails and compensates the run — swallowing
the exception lets `handle()` continue past a step that produced no result.
:::

:::warning Never catch control flow
Do not catch `DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FlowSuspended` (or any
`InternalFlowControl`) — those are the engine's suspend/replay signals, not errors. Business
exceptions (`ActionFailedException`, `FlowExpiredException`, `ChildWorkflowFailedException`, …) all
extend `FlowException` and are safe to catch; the two internal signals are the *only* things under
`…\Exceptions\Internal\`. If you use a broad `catch (\Throwable $e)`, re-throw control flow first:

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
