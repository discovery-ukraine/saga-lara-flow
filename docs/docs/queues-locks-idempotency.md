---
id: queues-locks-idempotency
title: Queues, locks & idempotency
sidebar_position: 15
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

## Never wrap an engine call in a transaction of your own {#host-transactions}

The guarantee above is read from the run's **recorded history**. A `DB::transaction()` of your own
around an engine call puts that history inside your rollback scope, so a rollback erases the record
while the work it describes has already happened. Measured identically on SQLite, MySQL and
PostgreSQL — this is the shape of the thing, not a driver quirk:

| the call inside your transaction | after your transaction rolls back |
| --- | --- |
| `runSync()` | the steps ran; **every row of the run is gone** |
| `compensate()` | the compensations ran; no `compensation_runs` row, the run left non-terminal |
| `signal()` | the delivery is gone; the wait is still open and nothing will wake the run |
| `cancel()` | the transition is gone; the run is where it was and keeps going |
| `run()` (queued) | nothing recorded and nothing dispatched — consistent, see below |

Only the queued `run()` is safe, and only while `saga-lara-flow.queue.after_commit` is on, as it is
by default: the job is held until your transaction commits, so a run either starts on committed data
or never starts at all. With it off the job goes out immediately, and whether it survives your
rollback is the queue driver's business rather than the engine's — leaving a job that may name a run
which was never committed.

Nothing raises. Every one of those calls returns normally and hands you a `FlowRun` describing a
state your own rollback then discarded.

### Why this is worse than losing the work

The rows are not bookkeeping — they are what stops the work happening twice. `compensate()` under a
rolled-back transaction leaves the run exactly where it was, so compensating it again runs every
compensation a second time:

```
compensate() inside a host transaction that rolls back  → undo:a executed
compensate() again, normally                            → undo:a executed
CompensationLog                                         → ["undo:a", "undo:a"]
compensation_runs rows                                  → 1
```

Two rollbacks of one step, and one record saying it happened once. The row that would have said
`undo:a` already ran is the row your rollback deleted.

### Why the engine cannot defend itself

Inside your transaction the engine's own `transaction()` is a **savepoint**, not a transaction
(measured: nesting level 2). It commits nothing, so durability is yours to decide, and the read-back
the engine uses to verify its own writes proves visibility on the connection rather than durability
— those are the same thing only while the engine's transaction is the outermost one. Getting out
would take a second connection, which then cannot see your uncommitted rows at all and would fence
against a run that, to it, does not exist. So this is a boundary the package documents rather than a
defect it is holding open.

### What to do instead

Commit your own work first, then call the engine — or use the queued `run()`, which does that
ordering for you. Where a step needs data you are about to write, pass it in as an argument or read
it inside the action, which runs after your transaction has closed.

## Locks

Concurrent drives of the *same* run are serialized by Laravel's `WithoutOverlapping` middleware:

```php
'locks' => [
    'enabled' => true,
    'workflow_ttl_seconds' => 900,
    'action_ttl_seconds' => 900,
    'compensation_ttl_seconds' => 900,
    'block_seconds' => 5,
    'prefix' => 'saga-lara-flow',
],
```

This guarantees that two workers can't advance one run at the same time. It covers workflow drives,
action steps (sequential and parallel) and compensations, each keyed on its own row. Each parameter:

- **`enabled`** — turn the `WithoutOverlapping` middleware on or off.
- **`store`** — cache store backing the locks (`null` = the app default). Point it at a dedicated
  store to isolate the locks from your app cache.
- **`workflow_ttl_seconds`** / **`action_ttl_seconds`** / **`compensation_ttl_seconds`** — **in
  seconds**. The maximum time a lock is held before it auto-expires, so a worker that dies mid-drive
  can't wedge a run forever. Set them comfortably above your longest workflow/action/compensation
  runtime.
- **`block_seconds`** — **in seconds**. How long a competing job waits to acquire the lock before
  giving up and letting the queue retry it later.
- **`prefix`** — string prefix for every lock key (namespacing when the store is shared).

### What the lock does not cover

The lock is job middleware, so it orders jobs against each other and nothing else. Three writers sit
outside it: `saga-flow:cancel`, a host calling `FlowHandle::cancel()` or `compensate()`, and the
monitor's sweep, which expires runs inline in `saga-flow:monitor`. A TTL is also a ceiling, not a
promise — a job that outruns `workflow_ttl_seconds` keeps working while its lock is released.

What covers those is the write itself. Every status write in the package — a run's transition, a step's
claim, an outcome — is a conditional `UPDATE` on the value its caller read before deciding, which is
the only form every supported driver enforces atomically. A writer whose row has moved on writes
nothing and is told so, instead of overwriting whoever got there first:

```php
try {
    SagaFlow::loadFlow($runId)->cancel('superseded');
} catch (ConcurrentFlowTransitionException $e) {
    // The run moved while you were deciding — the message names where it is now.
}
```

The engine absorbs the same refusal on its own paths: a drive that loses simply stops, so the job
ends cleanly rather than retrying.

Two workers that both legitimately hold a run as `running` are a different problem — nothing changes
between them for a condition to catch. That is what the lock is for.

## When a step is quietly skipped

Several things can happen to a worker without failing its job: it loses the claim to whoever already
owns the row, its claim turns out not to have survived its own transaction, it finishes but has been
superseded meanwhile so its outcome is rejected, a write to the step is refused because the row
moved on, or it finishes a parallel step whose batch a duplicate had already closed. Most are
ordinary consequences of at-least-once delivery — a refused write can equally come from an operator
or from terminal settlement — and none leaves a trace in the run's history, so the package keeps a
second journal for them:

```php
'logging' => [
    'anomaly_level' => env('SAGA_LARA_FLOW_ANOMALY_LOG_LEVEL', 'info'), // null = off
    'channel' => env('SAGA_LARA_FLOW_LOG_CHANNEL'),                     // null = app default
],
```

Grep for `claim_lost`, `outcome_rejected`, `batch_finished_early` and `write_refused`; each line
carries the run id, row id, sequence and class, and `write_refused` adds the `site` of the write that
was refused. `claim_not_committed` joins them when a claim did not survive its own
transaction, which is a defect in listener or observer code rather than a race, `expiry_failed`
when the sweep could not plan an overdue run's rollback and stepped over it, and
`rejection_undelivered` when the event below could not be handed to a listener. A refused run
transition is journalled here too, as `transition_lost`,
carrying the status observed, the one intended and the one actually holding the row — it is the one
entry that is not always quiet, since `FlowHandle` raises rather than absorbing it. None of them are
written to `flow_events`, which records the run's business history — an abandoned attempt changed
nothing in it. See [Reclaim & recovery](./reclaim-and-recovery.md) for what each one means and what
to do about it.

A rejected outcome is the only one that discards something the work produced, so it also raises an
event carrying it — see [A refused outcome](./events.md#refused-outcome).
`rejection_undelivered` is that hand-over failing, and the one way the payload is lost anyway.

:::tip A lock TTL is not the same as reclaim
`action_ttl_seconds` does not answer "when can a stuck `Running` step be retried". It governs a
**cache key**, not the database row, and stops applying at all when `enabled` is `false`. The
mechanism for the row itself is `reclaim` — see [Reclaim & recovery](./reclaim-and-recovery.md),
which compares it against every other "how long before we act" dial in the package.
:::

## Determinism is the contract

Idempotency relies on `handle()` being deterministic. If a replay diverges from the recorded history
(a step appears that wasn't there before, or in a different order), the engine raises
`HistoryContractMismatchException`. See [Determinism rules](./determinism-rules.md).
