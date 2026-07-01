---
id: queues-locks-idempotency
title: Queues, locks & idempotency
sidebar_position: 14
---

# Queues, locks & idempotency

## How a run is driven

Every workflow and action runs as a queued job on the configured connection/queue. A run advances by
**replaying** `handle()` against its recorded history: completed operations return their stored
results, and execution proceeds until it hits the next un-run operation (which is dispatched) or a
suspension point (a signal wait, a queued action). Each operation is identified by a deterministic
`(flow_run_id, sequence)` pair.

## Idempotency

Because every operation is keyed by `sequence` and side effects are recorded once, **re-driving a run
reproduces the same final state**. A job delivered twice, a worker that restarts mid-flight, a
manual `kick` — none of them double-charge a card or double-ship an order, because the already-run
step is reused from history rather than executed again.

## Locks

Concurrent drives of the *same* run are serialized by Laravel's `WithoutOverlapping` middleware:

```php
'locks' => [
    'enabled' => true,
    'workflow_ttl_seconds' => 900,
    'action_ttl_seconds' => 900,
    'block_seconds' => 5,
    'prefix' => 'saga-lara-flow',
],
```

This guarantees that two workers can't advance one run at the same time. TTLs bound how long a lock
is held if a worker dies; `block_seconds` is how long a competing job waits for the lock before
giving up and letting the queue retry it. Set `store` to a dedicated cache store if you want the
locks isolated from your app cache.

## Determinism is the contract

Idempotency relies on `handle()` being deterministic. If a replay diverges from the recorded history
(a step appears that wasn't there before, or in a different order), the engine raises
`HistoryContractMismatchException`. See [Determinism rules](./determinism-rules.md).
