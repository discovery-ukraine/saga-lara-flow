# Upgrading

## From 1.1.x to 1.2.0

> ### ⚠️ Run `php artisan migrate` after upgrading  Four migrations ship with this release —
> `add_reclaim_stale_running_columns`, `index_signal_waits`, `unique_flow_tag_keys` and
> `add_expiry_backoff_to_flow_runs` — and the engine writes their columns from the moment it starts.
> Deploy them together with the code. They run from the package — do **not** `vendor:publish` them,
> or `migrate` would try both copies. `unique_flow_tag_keys` deletes rows; read its item below
> before you run it.

### Action required

#### Decide how a killed worker's step is recovered

**Likelihood of impact: high.** A row that is already `Running` is no longer picked up by a
redelivered job, so a worker killed mid-execution leaves a step nothing brings back — replay parks
the run, and so does `saga-flow:kick`. Turn on one of the two recoveries, or accept that such a run
waits for a human:

- **`reclaim`** — the row becomes claimable again after its window and the step **runs again**. Off
  by default; read the [reclaim guide](https://sagalaraflow.dev/reclaim-and-recovery) first.
- **The monitor** — set `monitor.expiration.defaults.action` (or `->expiresAt()`) and schedule
  `saga-flow:monitor`. The step is marked `Expired`; a required step then fails the run as a
  business error, while an optional one resolves through its fallback.

If you already run the doctor, its new R3 rule (`repair.redispatch_stale_running_actions`, default
`true`) redispatches **sequential actions only** — a parallel action or a compensation is bound to
its batch and is recovered only by that job being redelivered. It reaches such a row whatever set
its reclaim window, `reclaimStaleAfter()` included while the global switch is off, but not one whose
run may no longer start work, one still inside its `repair_available_at` backoff, or one that has
spent `repair.max_attempts`. A row past the cap stays as it is until a retry cycle reschedules it.

#### `unique_flow_tag_keys` deletes duplicate rows

**Likelihood of impact: high, if you have duplicates.** `flow_tags` uniqueness narrows from
`(flow_run_id, key, value)` to `(flow_run_id, key)`. For each pair the row with the highest
`updated_at` is kept — highest `id` breaking a tie — and the rest are deleted. The rollback restores
the old constraint but cannot bring rows back. Count them first; the table carries your configured
`database.table_prefix`, `saga_` by default:

```sql
-- MySQL
select flow_run_id, `key`, count(*) from saga_flow_tags
 group by flow_run_id, `key` having count(*) > 1;

-- PostgreSQL, SQLite
select flow_run_id, "key", count(*) from saga_flow_tags
 group by flow_run_id, "key" having count(*) > 1;
```

**Pause writes to `flow_tags` from the count until the migration finishes.** The count is only a
snapshot: two concurrent first writes for one key can still insert a pair under the old constraint
afterwards, and a write landing on an already-duplicated key between the winner being chosen and the
losers being deleted is lost without a trace. An empty count means there is nothing to lose *at that
moment*, not that the window is shut. This migration and `index_signal_waits` are both repeatable —
MySQL runs neither in a transaction, so one that died part-way can simply be repeated.

#### A scalar workflow result is no longer wrapped

**Likelihood of impact: medium.** A workflow result that *serialized* to a scalar had its value
stored as `{"value": ...}`, and nothing reversed the wrapper. It is now written straight through the
serializer, so `$run->result['value']` is just `$run->result` and `$this->child(...)->run()`
resolves what the child actually returned. What serialization produces is what counts: `null`,
arrays, models and `Arrayable` were never wrapped, while a `JsonSerializable` whose
`jsonSerialize()` returns a scalar was, and is affected like any other scalar.

Rows completed **before** the upgrade keep their envelope permanently — a wrapped `42` and a
workflow that returned `['value' => 42]` are stored identically, so nothing can unwrap them safely.
And because a parent replays from the top, one whose child completed before the upgrade reads the
wrapper on *every* later replay. Code consuming a scalar has to accept both shapes while such rows
are still read; draining the affected parents first avoids the mixed period.

#### Settle rows left by runs that finished before the upgrade

**Likelihood of impact: low.** Terminal runs now settle the steps and wait-markers they were
holding, but existing rows are not migrated — rewriting a host's history is not a migration's job.
They are harmless and no sweep selects them, but `FlowQuery::whereAwaitingSignal()` and
`whereAwaitingRetrySignal()` keep matching their runs until you clear them:

```sql
UPDATE saga_action_runs
SET status = 'cancelled'
WHERE status IN ('pending', 'running', 'awaiting_retry')
  AND flow_run_id IN (SELECT id
                      FROM saga_flow_runs
                      WHERE status IN ('completed', 'failed', 'cancelled', 'expired'));

UPDATE saga_flow_signals
SET status = 'cancelled'
WHERE status = 'waiting'
  AND wait_sequence IS NOT NULL
  AND flow_run_id IN (SELECT id
                      FROM saga_flow_runs
                      WHERE status IN ('completed', 'failed', 'cancelled', 'expired'));
```

Adjust the names for your `database.table_prefix`.

#### A `FlowRun` relation now reads in a defined order

**Likelihood of impact: low.** `actions()`, `compensations()`, `sideEffects()` and `children()` read
by `sequence`, `events()` by `recorded_at`, `signals()` and `tags()` by `id`. A read that appends
its own order, or pages by id, lands behind the default — clear it first. Eager loads,
`withCount()`, `whereHas()` and plain `chunk()` need nothing.

```php
$run->signals()->reorder()->orderByDesc('id')->first();
$run->actions()->reorder()->chunkById(500, fn ($actions) => ...);
```

#### Smaller adjustments

- **If you published the config file** *(medium)* — package config is merged only at the top level,
  so an array of your own is taken as it stands. Add `locks.compensation_ttl_seconds` (default 900)
  and `monitor.expiration.backoff` (`60` doubling to `3600`). A missing compensation TTL inherits
  `action_ttl_seconds`, so nothing breaks if you forget.
- **If you catch around `FlowHandle::cancel()` or `compensate()`** *(medium)* — both can now also
  raise `ConcurrentFlowTransitionException`, when the run moved underneath the handle. Catch it
  alongside `CannotCancelTerminalFlowException`, which stays distinct because it means the handle
  already knew the run was finished. Neither extends `InvalidTransitionException`, deliberately:
  that one means the state graph forbids the move, whereas a lost race is worth re-reading and
  retrying.
- **If you observe the package's Eloquent models** *(medium)* — every status write is now a
  conditional `UPDATE`, so an observer on a swapped-in `models.flow_run`, `models.action_run` or
  `models.compensation_run` no longer sees `updating`/`updated` for a transition. Most publish a
  package event and a `flow_events` entry instead; listen to those. Four write no replacement at
  all, by design — settling a parked step, recording the queue's exhausted attempts, and the two
  settlements a terminal run makes over its open steps and waits — so auditing those needs another
  mechanism. A listener that swallows a failed query now stops the step rather than letting it run
  unrecorded (`claim_not_committed`).
- **If you swapped in your own `models.flow_tag`** *(low)* — extend `Models\FlowTag` rather than
  reimplementing it: the int-to-string normalisation of a tag value now also lives on that model as
  an `AsTagValue` cast, so a replacement without it stores int values unnormalised.
- **If you `match` over the status enums** *(medium)* — `ActionStatus::Cancelled` is now written and
  `SignalStatus::Cancelled` is new, so a `match` that was exhaustive needs the new arm. See
  [Statuses](https://sagalaraflow.dev/statuses).
- **If you query step or signal status directly** *(medium)* — a dashboard counting `action_runs`
  where `status = 'running'` stops counting runs that ended; those counts were wrong. A query that
  compensated by joining `flow_runs` is now redundant rather than harmful.
- **If you set a long `database.table_prefix`** *(low)* — it is capped at **24 bytes**, 24 ASCII
  characters and fewer otherwise; the default `saga_` is five. Six indexes are named explicitly
  rather than derived to reach it. A database that failed part-way on a too-long prefix needs its
  package tables dropped before `migrate` will get past the first.
- **If you override `Workflow::action()` to return your own builder** *(low)* — `retryOnSignal()`
  widens its first parameter to `RetryPolicy|string $signal` and appends a fifth `?Closure $when`.
  PHP refuses an override whose signature no longer matches, so widen yours on both `ActionBuilder`
  and `SagaStepBuilder`.
- **If you call `FlowHandle::compensate()`** *(medium)* — planning the rollback replays `handle()`,
  and that replay no longer settles a spent optional step (so no `OptionalActionFailed`) nor
  schedules a parallel block it had only reached to read. It still writes tags, because a workflow
  branching on its own tag would otherwise plan a different stack. Six exception classes end the
  plan; any other throw now surfaces out of `compensate()` with the run untouched, rather than
  rolling back a truncated plan and reporting it complete — catch it if you did not before. The
  expiry sweep absorbs the same throw instead, journalling `expiry_failed`.
- **If you call the runtime classes directly** *(low)* — they are `@internal`, but an
  `ActionRunRepository` implementation must add `dueForStaleRunningRepair()`, taking a limit and a
  max-attempts count and returning `iterable`; `ActionDispatcher::dispatch()`, `runInline()` and
  `ActionRecorder::scheduleAction()` take a readonly `Data\ActionSchedule` whose named parameters
  are the old tail of optional arguments; and `execute()` on `ActionDispatcher` and
  `CompensationExecutor`, plus `runInline()`, return `Enums\StepExecution` — `Executed`, `ClaimLost`
  (nothing ran) or `Superseded` (it ran, but the row changed hands first) — rather than `void`. Call
  `->settled()` if the distinction does not matter.

### Behaviour changed

Nothing below asks anything of you. Each links to the page that covers it.

- **A recorded outcome is never demoted.** Outcome writes are fenced against the claim that produced
  them, so a straggler cannot flip a succeeded step to `Failed`. Expect `outcome_rejected` lines
  when runs are cancelled while steps are in flight: a refused outcome is not stored, and that line
  is what records it. [Reclaim & recovery](https://sagalaraflow.dev/reclaim-and-recovery)
- **A run transition is refused if the row moved on**, rather than the last writer winning. A drive,
  an expiry and a queued rollback continuation absorb the refusal and stop; `FlowHandle` raises it,
  and a child-close job rethrows while the child is still live so the close is retried.
  [Queues, locks & idempotency](https://sagalaraflow.dev/queues-locks-idempotency)
- **A finished run settles what it was holding.** Steps in `pending`, `running` or `awaiting_retry`
  become `cancelled`, as do open wait-markers; steps that reached an outcome are untouched.
  [Statuses](https://sagalaraflow.dev/statuses)
- **No sweep selects work belonging to a finished run.** The doctor's two scans and the monitor's
  two each exclude it, so an unsettled row can no longer hold the head of every batch.
  [Expiration & monitoring](https://sagalaraflow.dev/expiration-and-monitoring)
- **No step starts under a run that is rolling back.** A queued step, a doctor redispatch and a
  signal-gated retry are all refused under `Cancelling`, where one could previously complete after
  the stack to undo had been planned. Settling a row already started is unchanged.
  [Statuses](https://sagalaraflow.dev/statuses)
- **A rollback no longer stops at a child that already finished.** A parent that carried on past a
  failed child under `->continueParentOnFailure()` used to roll back only the steps before it and
  report that as complete, so a compensation you never saw run may start running.
  [Child workflows](https://sagalaraflow.dev/child-workflows)
- **An unfinished `Stop`-policy compensation stops a rollback.** One left `Pending` or `Running`
  when its level finishes is treated the way a failed one is, and recorded under
  `flow_run.exception['compensation']`. A `Continue`-policy compensation still does not halt the
  unwind. [Sagas & compensation](https://sagalaraflow.dev/sagas-and-compensation)
- **A run whose rollback the sweep cannot plan steps aside** for a widening window instead of
  holding its place in the page. There is no attempt cap — the cause can be temporary.
  [Expiration & monitoring](https://sagalaraflow.dev/expiration-and-monitoring)
- **Five recorder transitions write conditionally** — a give-up, a park, a rewind, settling a parked
  step, and the queue's exhausted-attempts flag. One that loses writes nothing, journals
  `write_refused` with a `site`, and returns `false` where it returned `void`.
- **`RunCompensationJob` takes its own queue lock**, which compensations previously had none of.

### Additions

Nothing to do; each is opt-in or additive.

- **`reclaim`** (`actions.reclaim.stale_running`, `sagas.reclaim.stale_running`) lets a `Running`
  row be claimed again once its window has passed, recognising a worker that died mid-execution. Off
  by default; per-step via `reclaimStaleAfter()` and `enableStaleReclaim()`, and per-compensation
  via `reclaimCompensationStaleAfter()` and `enableCompensationStaleReclaim()`.
- **`logging`** — `anomaly_level` (default `info`, `null` to silence) and `channel`. The package's
  first logging: a second journal beside `flow_events` for the refusals nothing else records.
  Writing is best-effort and never fails a job.
- **`RetryPolicy`** — `retryOnSignal()` accepts a policy object, and a `when:` predicate gates
  whether *this* failure is worth parking. Every existing call site behaves identically.
  [Retry on signal](https://sagalaraflow.dev/retry-on-signal)
- **`ActionOutcomeRejected` and `CompensationOutcomeRejected`** carry what a refused outcome
  produced — the recorded form of the result, or the throw the engine does not rethrow. Nothing is
  stored and nothing about the run changes.
  [Events](https://sagalaraflow.dev/events#refused-outcome)
- **`CompensationStepStarted`** marks one compensation beginning, distinct from
  `CompensationStarted`, which marks the whole rollback once per run.
- **`FlowStatus::terminal()`** lists the four statuses a run never leaves; `isTerminal()` reads from
  it.
- **Two `FlowQuery` wait filters**, with the `index_signal_waits` indexes that serve them:
  `flow_signals` gains `(status, name, flow_run_id)`, `action_runs`
  `(status, retry_signal, flow_run_id)`.
- **`Models\FlowTag`** also normalises an int tag *value* to a string as an `AsTagValue` cast, so
  `FlowHandle::withTags()` and a direct model write get it too, not only `tag()`.

## From 1.0.x to 1.1.0

> ### ⚠️ Run `php artisan migrate` immediately after upgrading
>
> This is **not** a "the new feature stays unavailable until you migrate" situation. Every action the
> engine schedules — with or without a retry policy — writes the columns added by this release. An
> application that upgrades the package without running its migrations will break **ordinary workflow
> execution** with an unknown-column error on the very next action it schedules.
>
> ```bash
> composer update discovery-ukraine/saga-lara-flow
> php artisan migrate
> ```
>
> Deploy the two together. If your deploy runs migrations after the new code is already serving
> traffic, expect failures in that window.

### What's new

`retryOnSignal()` on an action (and on `saga()->step()`) parks a failed step until a named signal arrives, then re-runs
that step alone instead of failing the run and compensating it. See the
[Retry on signal](https://sagalaraflow.dev/retry-on-signal) guide.

Everything in this release is additive for workflows that never call `retryOnSignal()`. The three notes below are the
only places where existing behaviour changed.

### 1. The new migration

`add_retry_on_signal_to_action_runs` adds four nullable/defaulted columns to `action_runs`:
`retry_signal`, `retry_signal_attempts`, `retry_signal_max_attempts`, and
`queue_attempts_exhausted`. It runs from the package like the initial migration — do not
`vendor:publish` it.

### 2. `SignalRepository` gained two methods

If you bound your own implementation of `DiscoveryUkraine\SagaLaraFlow\Contracts\SignalRepository`, it must now also
implement:

```php
public function earliestPendingSince(string $flowRunId, string $name, ?DateTimeInterface $since): ?FlowSignal;

public function latestForSequence(string $flowRunId, int $sequence): ?FlowSignal;
```

The simplest fix is to extend `Repositories\EloquentSignalRepository` rather than implement the interface from scratch.

This is a minor release rather than a major one because the repository contracts are **not a public extension point**.
They are now marked `@internal`, and the supported way to change persistence behavior is
`config('saga-lara-flow.models.*')`, which is unaffected. Methods may be added to these contracts in future minor
releases on the same terms.

### 3. Two signal transitions no longer raise Eloquent model events

Delivering a signal into an open wait, and timing a wait out, are now written as single conditional
`UPDATE` statements instead of `save()` calls. That is the only form that is atomic on every supported driver, and it is
what keeps a delivery and a timeout from both claiming the same wait.

The consequence: an **observer** registered on a swapped-in `models.flow_signal` no longer sees
`updating`/`updated` for those two transitions (nor for the new "wait superseded" transition). If you rely on model
events for auditing or multi-tenancy bookkeeping, move that listener onto the package's own events —
`FlowSignalReceived`, `FlowSignalConsumed` — or read the `flow_events` log, both of which still record every transition.

### Recommended while you are here

- **Turn `repair.enabled` on.** The doctor is what recovers a step whose queue job was lost to a dying process. It is
  off by default and does nothing until you opt in.
- **Check your package-event listeners.** A synchronous listener that throws interrupts the engine's replay and can fail
  a healthy run. Mark them `ShouldQueue`, or make sure they cannot throw.
