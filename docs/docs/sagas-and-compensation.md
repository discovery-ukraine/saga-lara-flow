---
id: sagas-and-compensation
title: Sagas & compensations
sidebar_position: 6
---

# Sagas & compensations

The Saga pattern trades distributed transactions for **compensating actions**: each step registers
how to undo itself, and on failure the engine rolls completed steps back in **reverse order**.

## Action-level compensation

The primary style attaches an undo to each step:

```php
public function handle(string $orderId): void
{
    $this->action(ChargeCard::class, $orderId)
        ->compensateWith(RefundCard::class, $orderId)
        ->run();

    $this->action(ReserveStock::class, $orderId)
        ->compensateWith(ReleaseStock::class, $orderId)
        ->run();

    // If this throws, ReleaseStock then RefundCard run automatically.
    $this->action(ShipOrder::class, $orderId)->run();
}
```

Compensation can also be a closure:

```php
$this->action(MakeReservation::class, $id)
    ->compensateWith(fn () => Reservation::release($id))
    ->run();
```

## Grouped sagas

`saga()` expresses a compensation boundary explicitly and exposes group-level policies:

```php
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationFailurePolicy;

$this->saga()
    ->onCompensationFailure(CompensationFailurePolicy::Continue)
    ->compensateInParallel()
    ->step(ChargeCard::class, $orderId)->compensateWith(RefundCard::class, $orderId)
    ->step(ReserveStock::class, $orderId)->compensateWith(ReleaseStock::class, $orderId)
    ->run();
```

- `compensateInParallel()` runs the group's undos concurrently (a single rollback level: together via
  `Bus::batch` when queued, sequentially under `runSync`).
- `compensateStepOnSelfFailure()` also compensates a step that *itself* failed (for non-atomic
  actions that may leave partial effects) — such compensations must be idempotent.

## Failure policies

`CompensationFailurePolicy`:

- `Stop` (default) — halt the rollback on the first failed compensation.
- `Continue` — keep rolling back even if one undo fails.

Precedence is **action > group > config** (`sagas.default_compensation_failure_policy`). If a
compensation itself fails under `Stop`, a `CompensationFailedException` surfaces.

## Manual compensation

You can trigger a rollback from outside the workflow through the handle:

```php
SagaFlow::loadFlow($runId)->compensate(); // roll back completed steps, then cancel
```
