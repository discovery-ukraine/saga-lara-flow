---
id: parallel
title: Parallel actions
sidebar_position: 9
---

# Parallel actions

`parallel()` runs several actions concurrently — as queued jobs, or inline under `runSync` — and
returns their results as a list in declaration order:

```php
[$pricing, $inventory, $reviews] = $this->parallel()
    ->action(FetchPricing::class, $sku)
    ->action(FetchInventory::class, $sku)
    ->action(FetchReviews::class, $sku)
    ->failFast()
    ->run();
```

## Failure policy

`ParallelFailurePolicy` controls what happens when a step fails:

- `failFast()` (default, `ParallelFailurePolicy::FailFast`) — cancel the block on the first hard
  failure; pending siblings never start.
- `waitAllThenFail()` — let every step settle before the block fails.

The default comes from `parallel.default_failure_policy` in config.

## Per-step options

Steps in a parallel block are full builders — they can carry compensations, deadlines, and optional
behaviour:

```php
[$a, $b] = $this->parallel()
    ->action(ReserveSeat::class, $id)
        ->compensateWith(ReleaseSeat::class, $id)
    ->optionalAction(FetchRecommendations::class, $id)
        ->fallbackValueOnFail([])
    ->waitAllThenFail()
    ->run();
```

When a parallel block fails, any completed steps that registered compensations are rolled back, just
like a sequential saga. See [Sagas & compensations](./sagas-and-compensation.md) and
[Optional actions](./optional-actions.md).
