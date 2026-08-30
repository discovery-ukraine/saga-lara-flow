---
id: reclaim-and-recovery
title: Reclaim & recovery
sidebar_position: 15.5
---

# Reclaim & recovery

**Reclaim** answers one question no other setting in the package does: may a step still marked `Running` be executed
again? This page covers it in full, and starts by placing it against the three other "how long before we act" dials it
is easily confused with.

## The four dials, side by side

|                            | Business deadline (`expires_at`)                                             | Lock TTL (`locks.*_ttl_seconds`)                | Repair grace (`repair.grace_seconds`)                               | Reclaim (`reclaim.stale_running`)                                                                                                                              |
|----------------------------|------------------------------------------------------------------------------|-------------------------------------------------|---------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Answers                    | "Has this taken too long, business-wise?"                                    | "Can the cache lock be considered abandoned?"   | "Has a *Pending* step sat long enough to suspect its job was lost?" | "Has a *Running* row sat long enough to suspect its worker died?"                                                                                              |
| Lives on                   | `action_runs.expires_at` / `flow_runs.expires_at`                            | A cache key, not a database row                 | Used only inside the doctor, against `created_at`                   | `reclaim_stale_after_seconds` (the configured window) and `reclaim_stale_at` (the deadline the claim derives from it), on `action_runs` / `compensation_runs`   |
| Default                    | Off (`null`)                                                                 | 900 seconds                                     | 60 seconds                                                          | Off                                                                                                                                                            |
| Enforced by                | The opt-in monitor sweep (`saga-flow:monitor`)                               | Laravel's `WithoutOverlapping` queue middleware | The opt-in doctor (`repair.enabled`)                                | The claim itself (`ActionRecorder::startAction()` / `CompensationRecorder::startCompensation()`); the doctor can additionally act on it                        |
| What happens once it fires | The step/run is marked `Expired` — a give-up, surfaced as a business failure | Laravel lets a competing job proceed            | The doctor re-dispatches a fresh job for the same row               | A `Running` row becomes claimable again by the next thing that claims it                                                                                       |
| Independent of the others? | Yes                                                                          | Yes                                             | Yes                                                                 | Yes                                                                                                                                                            |

## Not the same as a lock TTL

`locks.action_ttl_seconds` is the closest of the three in spirit — "the maximum time a lock is held before it
auto-expires, so a worker that dies mid-drive can't wedge a run forever" — but it operates at a different layer:

- The lock TTL belongs to a **cache key** (`WithoutOverlapping`), created and read by Laravel's queue middleware. It
  says nothing about the database row, and it stops existing entirely when `locks.enabled` is `false`.
- Reclaim's threshold belongs to the **row**, is read by the atomic claim in `ActionRecorder` /
  `CompensationRecorder`, and applies whether or not locks are enabled.

Turning locks off therefore has no effect on whether stale `Running` rows can be recovered, and vice versa.

## What happens with reclaim off

Reclaim is off by default, and the default has a consequence worth knowing before you leave it there.

The claim accepts a row that is `Pending` — and, for an action, `Failed`, which is what the row shows between two of
the action's own native `$tries`. A row already `Running` is accepted only once its reclaim deadline has passed, and
with reclaim off there is no deadline, so it is never accepted. A worker killed mid-execution (an evicted pod, the OOM
killer, a `SIGKILL` deploy) leaves its row `Running` for good: replay reads a `Running` step as still in flight and
parks the run, `saga-flow:kick` does the same, and the run waits indefinitely.

What that buys is that the engine never re-executes a step on its own initiative:

| Race                                                            | Who creates the duplicate | Prevented?             |
|-----------------------------------------------------------------|---------------------------|------------------------|
| A job left over from a superseded retry cycle                   | the engine                | Yes                    |
| A job racing a row the monitor just expired                     | the engine                | Yes                    |
| A job the doctor dispatched on top of a live one                | the engine                | Yes                    |
| The queue redelivering a job while the first attempt still runs | the queue driver          | No — and no engine can |

The last row is the at-least-once behaviour of SQS visibility timeouts, `retry_after` on Redis/database, and every
managed queue: a redelivery means the driver decided to retry, never that the first worker is dead. Nothing outside the
driver can tell those apart, which is why [Queues, locks & idempotency](./queues-locks-idempotency.md) requires action
code to tolerate running twice.

### Two ways to recover a killed worker's step

Both are opt-in, and they do different things.

- **Reclaim** (this page) — the row becomes claimable again once its deadline passes, and the step **runs again**. This
  recovers the work.
- **The monitor** — set `monitor.expiration.defaults.action` (or an explicit `->expiresAt()`) and schedule
  `saga-flow:monitor`. The stuck step is marked `Expired` and the run **fails as a business error** rather than hanging.
  This recovers the run, not the work. See [Expiration & monitoring](./expiration-and-monitoring.md).

With both off, the run waits for a human.

## Turning it on

Actions and compensations are switched independently:

```php
'actions' => [
    'reclaim' => [
        'stale_running' => [
            'enabled' => false,      // off by default — a Running row is never re-claimed
            'after_seconds' => 900,  // used whenever enabled resolves true, globally or per step
        ],
    ],
],

'sagas' => [
    'reclaim' => [
        'stale_running' => [
            'enabled' => false,
            'after_seconds' => 900,
        ],
    ],
],
```

Per-step overrides on `ActionBuilder` (and mirrored on `SagaStepBuilder` for a step inside a `saga()` group):

```php
$this->action(ChargeCard::class, $orderId)
    ->reclaimStaleAfter(1200)          // set the threshold for THIS step (and enable it)
    ->run();

$this->action(SendWelcomeEmail::class, $userId)
    ->enableStaleReclaim(false)        // force it off for this step, even if config enables it globally
    ->run();

$this->action(ChargeCard::class, $orderId)
    ->enableStaleReclaim(true)         // force it on for this step (using config's after_seconds),
    ->run();                           // even if config disables it globally
```

The compensation registered on a step gets the identical pair of methods, scoped separately:

```php
$this->action(ChargeCard::class, $orderId)
    ->compensateWith(RefundCard::class, $orderId)
    ->reclaimCompensationStaleAfter(600)
    ->enableCompensationStaleReclaim(true)
    ->run();
```

Precedence, resolved once at schedule time and persisted onto the row (the same shape as `expires_at`'s configured
defaults):

1. An explicit threshold (`reclaimStaleAfter()` / `reclaimCompensationStaleAfter()`) wins outright, and implies
   "enabled".
2. Otherwise, an explicit `enableStaleReclaim(true)` / `enableCompensationStaleReclaim(true)` uses config's
   `after_seconds`, **regardless of config's own `enabled` value**.
3. Otherwise, `enableStaleReclaim(false)` forces the row's threshold to `null` (off), regardless of config.
4. With no override at all, the row inherits config.

## What changes once it's on

A `Running` row becomes claimable again once its deadline passes — but only when something actually *claims* it, which
means one of two things: a redelivered queue job for that row, or the doctor (next section). Reclaim is otherwise
passive; it does not go looking for stale rows.

`saga-flow:kick` is not one of those two. It re-wakes the run, and the replay it triggers reads a `Running` step as
still in flight and parks the run again without reaching the claim. Kick recovers a run whose *resume* was lost, not a
row whose *worker* was.

## The doctor's active side (R3)

The opt-in doctor (`repair.enabled`) carries a third rule alongside R1 and R2:

```php
'repair' => [
    // ...
    'redispatch_stale_running_actions' => true,  // R3
],
```

R3 finds ordinary (non-parallel) `Running` actions past their reclaim deadline and re-dispatches a fresh job for them —
the same shape as R1 (lost `Pending` actions) for a different cause: a worker that died mid-execution rather than a job
that never arrived. It needs `repair.enabled`, and it acts on any row carrying a reclaim deadline.

That includes a row enabled **per step** while the global switch is off: a step calling `reclaimStaleAfter()` opts
itself in, and participates in reclaim fully — there is no passive-only setting. With reclaim configured nowhere, no row
carries a deadline and R3 has nothing to act on.

**Parallel actions and compensations are out of scope for R3.** Recovering one would mean adding a job back into its
`Bus::batch`, which needs that batch's id; the package does not store it, and the only way to recover it is to look its
deterministic name up in Laravel's `job_batches` table — a detail of the *database* batch driver, not something every
host has. For those two, reclaim stays passive even with the doctor enabled: a redelivery of the row's own job is what
reclaims them.

## Two live workers, one row

Reclaim exists to let a second worker take over a row whose first worker looks dead, and it cannot verify that it *is*
dead. Enabling reclaim therefore means two workers may occasionally execute the same step at once — the same property
at-least-once delivery already has.

Only one of them can record an outcome. Each claim increments the row's `attempts`, and every outcome write is
conditional on two things: the `attempts` value its own claim produced, and the row still being `Running`. A superseded
worker's write updates no rows, fires no event, and does not fail its job — it is logged and dropped.

The two conditions guard two different rivals:

- **`attempts`** fences against another executor — the straggler above.
- **The status** fences against a settlement that never claimed the row at all. The monitor expiring an overdue step
  does not touch `attempts`, so without it a late worker would overwrite that `Expired` with a `Completed` and leave the
  run carrying both. The expiry write is conditional in the same way, so a step that completes just before the sweep
  reaches it is not demoted to `Expired`.

Together they give one guarantee: **a recorded terminal state is never overwritten**. A straggler that eventually throws
cannot flip a step the live worker already completed to `Failed` and send the saga rolling back over work that
succeeded — refunding a payment that went through. Nor can a straggler's stale result replace the current one, nor a
sweep undo a result that beat it by a moment.

Compensations carry the same `attempts` counter and the same fence.

## When a duplicate closes a batch early

Parallel blocks and rollback levels run as a `Bus::batch`, which Laravel closes once every job in it has reported. A
duplicate delivery breaks that arithmetic: it loses its claim, returns quietly and successfully, and the count reaches
zero while the live worker is still executing. The batch's callback fires early, and Laravel does not fire it a second
time — the condition is `(pendingJobs - failedJobs) === 0`, and the next report takes the count to `-1`.

The `WithoutOverlapping` lock prevents this: a duplicate arriving while the first worker holds the lock is released back
to the queue instead of completing. It is reachable only with `locks.enabled` set to `false`, or after a lock's TTL has
passed.

The consequence differs by half of the engine:

- **Parallel actions** — the worker that does finish sees the batch already closed, logs `batch_finished_early`, and
  sends the wake itself. Otherwise the run would park on a step that is `Running` at replay time with no wake left.
- **Rollback levels** — the level is treated as stopped, as it would be for a compensation whose worker died; from the
  outside the two are indistinguishable. `CompensationUnfinishedException` reports that the compensation *had not
  finished when its level ended*, which is what was observed.

## Finding these races in your logs

A lost claim, a rejected outcome and an early-closed batch are all normal and all silent. The package keeps a second
journal for them — `AnomalyLog`, alongside the `flow_events` business history:

```php
'logging' => [
    'anomaly_level' => env('SAGA_LARA_FLOW_ANOMALY_LOG_LEVEL', 'info'), // null = off
    'channel' => env('SAGA_LARA_FLOW_LOG_CHANNEL'),                     // null = app default
],
```

Six reason codes to grep for. All but one carry the run id, row id, sequence and class; `expiry_failed`
is about a run rather than a row, and carries the run id, its workflow class and the throw:

- **`claim_lost`** — a worker found the row already owned and did not execute the step.
- **`outcome_rejected`** — a worker finished, but the row had changed hands and its result was dropped.
- **`batch_finished_early`** — a parallel step completed after a duplicate delivery had closed its batch.
- **`claim_not_committed`** — a claim was written and was gone once its transaction closed. The line carries both
  attempt counts, the claimed one and the one the row actually holds.
- **`expiry_failed`** — the sweep could not plan an overdue run's rollback, so it left the run alone and moved on to
  the next. The line carries the throw. The run stays overdue and is tried again on the next sweep.
- **`write_refused`** — a write to a step was refused because the row had moved on since it was read, or the run under
  it had finished. The line carries a `site` naming which write it was.

Raise the level to `warning` to surface them in your alerting. A steady stream of the first three points to a queue
timeout tuned shorter than the work takes, or to locks being off.

`write_refused` is a race like the first three, and the ordinary one: terminal settlement runs once, so anything
written to a step afterwards would stand for ever with nothing left to notice it. A steady stream of it on runs nobody
cancelled points the same way as `claim_lost` — a queue timeout tuned shorter than the work takes.

The remaining two are not races and do not come from tuning. `expiry_failed` means the run's own `handle()` threw
while the sweep was replaying it to find the compensations — a workflow reading something that has since gone, most
often. Until
that is fixed the run cannot be expired, but nothing else in the sweep is held up by it.

`claim_not_committed` is the other one: something ran inside the engine's own transaction and left it
unusable. On PostgreSQL a single failed statement aborts a transaction and turns the eventual commit into a rollback
while still reporting success, so a listener on `ActionStarted` or a model observer that runs a failing query and
swallows it discards the claim without anything raising. The claim is read back after the transaction closes for
exactly this reason, and the step does not run. Fix the listener; nothing on the engine's side is tunable here.

## Observability

Each claim fires a package event and appends a `flow_events` entry:

- `ActionStarted` / `action.started`.
- `CompensationStepStarted` / `compensation.step_started` — one per compensation row. Distinct from
  `CompensationStarted` (`compensation.started`), which marks the whole rollback beginning, once per run.

Both claims are written as a single conditional `UPDATE`, the same compare-and-swap shape used for signal delivery, so
neither raises Eloquent model events on `models.action_run` / `models.compensation_run`. The events above are the
supported way to observe them. See [Queues, locks & idempotency](./queues-locks-idempotency.md).
