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

The engine guarantees one thing precisely: a step that has **completed and recorded its result** is
never executed again. On any re-drive — a job delivered twice, a worker that restarts mid-flight, a
manual `kick` — that step's stored result is reused from history instead of being re-run, keyed by
`(flow_run_id, sequence)`. In that sense, re-driving a run converges to the same final state.

:::caution This is not automatic end-to-end idempotency
The guarantee covers **recorded** steps only — it does **not** make the work *inside* an action
idempotent. If a job hangs, is retried by the queue, or dies *after* performing its external effect
(charging a card, calling an API) but *before* recording its result and waking the flow, that effect
can happen more than once. The engine will happily reuse a step once it is recorded, but it cannot
un-charge a card that was charged by a job that never got to write its result.

So end-to-end idempotency depends on **your action code**. Make each action safe to retry:

- Use an idempotency key (many payment/HTTP APIs accept one) so the provider deduplicates.
- Prefer upserts / conditional writes over blind inserts.
- Check whether the effect already happened before repeating it.

The `(flow_run_id, sequence)` pair is a natural, stable idempotency key to hand to downstream systems.
:::

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

This guarantees that two workers can't advance one run at the same time. Each parameter:

- **`enabled`** — turn the `WithoutOverlapping` middleware on or off.
- **`store`** — cache store backing the locks (`null` = the app default). Point it at a dedicated
  store to isolate the locks from your app cache.
- **`workflow_ttl_seconds`** / **`action_ttl_seconds`** — **in seconds**. The maximum time a lock is
  held before it auto-expires, so a worker that dies mid-drive can't wedge a run forever. Set them
  comfortably above your longest workflow/action runtime.
- **`block_seconds`** — **in seconds**. How long a competing job waits to acquire the lock before
  giving up and letting the queue retry it later.
- **`prefix`** — string prefix for every lock key (namespacing when the store is shared).

## Determinism is the contract

Idempotency relies on `handle()` being deterministic. If a replay diverges from the recorded history
(a step appears that wasn't there before, or in a different order), the engine raises
`HistoryContractMismatchException`. See [Determinism rules](./determinism-rules.md).
