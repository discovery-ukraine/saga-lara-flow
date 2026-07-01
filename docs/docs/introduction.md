---
id: introduction
title: Introduction
slug: /
sidebar_position: 1
---

# Saga Lara Flow

**Saga Lara Flow** is a workflow management engine with an integrated **Saga pattern**, built on top
of Laravel Queues.

It lets you write a long-running, durable business process as a single
deterministic PHP method: each step runs, is recorded, and survives worker
restarts through exception-based suspension and replay. When a step fails partway
through, registered **compensations** roll back the completed work in reverse
order.

It is inspired by
[Durable Workflow (formerly Laravel Workflow)](https://github.com/durable-workflow/workflow),
but it is not a replacement for it. Saga Lara Flow positions itself as a lighter,
native-Laravel alternative: no Fibers, generators, or promises — just queues, an
event log, and Eloquent.

```php
use DiscoveryUkraine\SagaLaraFlow\Workflow;

class CheckoutWorkflow extends Workflow
{
    public function handle(string $orderId): array
    {
        $charge = $this->action(ChargeCard::class, $orderId)
            ->compensateWith(RefundCard::class, $orderId)
            ->run();

        $this->action(ReserveStock::class, $orderId)
            ->compensateWith(ReleaseStock::class, $orderId)
            ->run();

        $this->action(ShipOrder::class, $orderId)->run();

        return ['charge' => $charge];
    }
}
```

```php
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;

$run = SagaFlow::create(CheckoutWorkflow::class)
    ->withArguments('order-42')
    ->run();
```

If `ReserveStock` or `ShipOrder` fails, `RefundCard` (and any other registered compensation) runs
automatically, in reverse order.

## Why it exists

Distributed business processes — checkout, provisioning, onboarding — span multiple services and
can't rely on a single database transaction. The Saga pattern replaces the transaction with
**compensating actions**: each step knows how to undo itself, and the engine rolls back on failure.
Saga Lara Flow gives you that on top of the queue infrastructure you already run.

## Core ideas

- **Deterministic `handle()`.** Your workflow method is replayed from the start on every resume; the
  engine reuses recorded results instead of re-running completed work.
- **`(flow_run_id, sequence)` identity.** Every operation is keyed by a deterministic ordinal, which
  drives replay, idempotency, and the history contract.
- **Exception-based suspension.** Waiting for a signal or a queued action suspends the workflow by
  throwing an internal control-flow exception; the engine resumes it later.
- **Compensation.** Steps register undo logic that runs in reverse order when a later step fails.

## When should I use Durable Workflow instead?

Saga Lara Flow is intentionally a lighter, Laravel-native package. It is focused
on queues, Eloquent, an event log, replay, signals, child workflows, and
first-class Saga compensations inside a single Laravel application.

If you need a more complete workflow engine — SDK-neutral or polyglot workers,
standalone/external workers, Fiber-based execution, strict workflow-definition
fingerprinting, worker compatibility fleets, sticky execution, durable timers,
schedules, control-plane APIs, rich projections/observability, search attributes,
memos, history export/import, replay verification, external payload storage,
history budgets, or Temporal/Cadence-style operations — you should evaluate
[Durable Workflow](https://github.com/durable-workflow/workflow) instead.

## Where to go next

- [Installation](./installation.md)
- [Configuration](./configuration.md)
- [Your first workflow](./your-first-workflow.md)
