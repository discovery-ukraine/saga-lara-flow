---
id: optional-actions
title: Optional actions
sidebar_position: 10
---

# Optional actions

An **optional action** never fails the flow — its failure is swallowed and a fallback value is
returned instead:

```php
$score = $this->action(FetchRiskScore::class, $orderId)
    ->continueOnFailure()
    ->fallbackValueOnFail(0)
    ->run();
```

`optionalAction()` is a shorthand for `action()->continueOnFailure()`:

```php
$score = $this->optionalAction(FetchRiskScore::class, $orderId)
    ->fallbackValueOnFail(0)
    ->run();
```

If the action succeeds, `run()` returns its result; if it fails (after exhausting retries), `run()`
returns the fallback (`null` if none was set) and the workflow carries on.

## Declarative form

Mark an action optional at the class level:

```php
use DiscoveryUkraine\SagaLaraFlow\Attributes\ContinueOnFailure;

#[ContinueOnFailure]
class FetchRiskScore extends Action
{
    public function handle(string $orderId): int
    {
        // ...
    }
}
```

Optional actions work inside [parallel blocks](./parallel.md) too, letting a non-critical step fail
without tearing down the group.
