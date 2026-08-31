# Upgrading

## From 1.1.x to 1.2.0

> ### ⚠️ Run `php artisan migrate` immediately after upgrading
>
> Every action and compensation the engine schedules writes the columns added by this release. Deploy
> the migration together with the code, the same as every other release.

### What's new

`ActionRecorder::startAction()` and `CompensationRecorder::startCompensation()` now claim their row
atomically instead of writing to it unconditionally, closing a check-then-act race where a stale
job (a superseded retry cycle, a row the monitor just expired, or a job the doctor sent on top of a
live one) could execute a step a second time. See [Queues, locks & idempotency](https://sagalaraflow.dev/queues-locks-idempotency)
for the full picture, and the new **reclaim** section below for the opt-in half.

A companion mechanism, `actions.reclaim.stale_running` / `sagas.reclaim.stale_running`, lets a
`Running` row be claimed again once its reclaim window has passed — recognising a worker that died
mid-execution. Off by default; enable it globally or per action/compensation
(`reclaimStaleAfter()`, `enableStaleReclaim()`, and their `...Compensation...` mirrors on
`ActionBuilder`/`SagaStepBuilder`).

### 1. A `Running` row is no longer picked up by a redelivered job

This is the one change to be deliberate about, and it applies **whether or not you enable reclaim**.

The claim accepts a row that is `Pending` or `Failed`. A row that is already `Running` is accepted
only once its reclaim window has passed — and with reclaim off, the default, there is no window, so
it is never accepted. Previously any redelivered job would simply execute such a row again.

What that buys you: the engine's own machinery can no longer produce a duplicate execution. A job
left over from a superseded retry cycle, a job racing a row the monitor just expired, and a job the
doctor dispatched on top of one still running are all now refused. (What no engine can prevent is
the queue driver's own at-least-once delivery — SQS visibility timeouts, `retry_after` on
Redis/database — redelivering a job while the first attempt is still alive. That is the documented
baseline your action code already has to tolerate.)

What it costs you: if a worker is killed mid-execution — an evicted pod, the OOM killer, a `SIGKILL`
deploy — its row stays `Running`, and with everything at its default nothing brings it back. Replay
treats a `Running` step as still in flight and parks the run, and `saga-flow:kick` does the same, so
the run waits indefinitely. Choose one of the two recoveries if that matters to you:

- **`reclaim`** — the row becomes claimable again after its window, and the step **runs again**.
  Recovery of the work itself. See the [reclaim guide](https://sagalaraflow.dev/reclaim-and-recovery).
- **The monitor** — set `monitor.expiration.defaults.action` (or `->expiresAt()`) and schedule
  `saga-flow:monitor`. The step is marked `Expired` and the run **fails as a business error**
  instead of hanging. Recovery of the run, not of the work.

They are complementary: reclaim retries, the monitor gives up. Neither runs unless you turn it on.

### 2. The new migration

`add_reclaim_stale_running_columns` adds `reclaim_stale_after_seconds` (the configured window) and
`reclaim_stale_at` (the absolute deadline the claim derives from it) to both `action_runs` and
`compensation_runs`, plus an `attempts` counter on `compensation_runs` mirroring the one
`action_runs` already had. It runs from the package like every other migration — do not
`vendor:publish` it.

### 3. Status transitions no longer raise Eloquent model events

Every status write the engine makes is now a conditional `UPDATE` — the same shape (and for the same
reason: the only form every supported driver enforces atomically) as the signal transitions changed
in 1.1.0. That covers `startAction()` / `startCompensation()`, the four outcome writes
(`completeAction()`, `failAction()`, `completeCompensation()`, `failCompensation()`), and — see item
15 — every transition of a `flow_runs` row.

The consequence, exactly as documented in 1.1.0: an **observer** registered on a swapped-in
`models.flow_run`, `models.action_run` or `models.compensation_run` no longer sees
`updating`/`updated` for these transitions. Nothing is lost, though — each still records its own
package event and `flow_events` entry whenever it actually writes: the flow lifecycle events
(`FlowStarted`, `FlowWaiting`, `FlowResumed`, `FlowCompleted`, `FlowFailed`, `FlowCancelled`,
`FlowExpired`), `ActionStarted` (`action.started`, unchanged), `ActionCompleted`, `ActionFailed`,
`CompensationCompleted`, `CompensationFailed`, and the new `CompensationStepStarted`
(`compensation.step_started` — distinct from `CompensationStarted`, which marks the whole rollback
beginning once per run, not once per compensation). Listen to those, or read `flow_events`, instead
of Eloquent hooks.

### 4. A recorded outcome can no longer be overwritten by a superseded worker

Enabling reclaim lets a second worker take over a row whose first worker may be slow rather than
dead, so both can reach the end of the same step. The outcome writes are therefore fenced against
the claim that produced them (via `attempts`, which only the claim increments): an executor that has
been superseded updates no rows, records no history, and its job does not fail — it now hands what
it produced to `ActionOutcomeRejected` instead of dropping it (item 23). In practice this means
a **recorded success is never demoted** — a straggler that fails after the live worker succeeded can
no longer flip the step to `Failed` and send the saga rolling back over work that actually went
through.

The fence has a second condition alongside `attempts`: the row must still be `Running`. That guards
against a settlement that never claimed the row at all — the monitor expiring an overdue step does
not touch `attempts`. `ActionRecorder::expireAction()` is conditional for the same reason, from the
other side: a step that completes just before the sweep reaches it is not demoted to `Expired`. It
now returns `bool`, and the monitor counts and wakes only for an expiry it actually won.

Every one of these paths — a lost claim, a rejected outcome, a batch closed early, and now a refused
run transition (item 15) — is logged, since nothing else records them. The first three are also
quiet: they never fail a job. The fourth is the exception, and only on one surface — the engine
absorbs it, but `FlowHandle` lets it reach the caller, because an operator whose cancellation did not
happen has to be told. See `logging.anomaly_level` in item 7.

### 5. `ActionRunRepository` gained one method

If you bound your own implementation of `DiscoveryUkraine\SagaLaraFlow\Contracts\ActionRunRepository`, it must now also
implement:

```php
public function dueForStaleRunningRepair(int $limit, int $maxAttempts): iterable;
```

As with the 1.1.0 note on `SignalRepository`, the simplest fix is to extend
`Repositories\EloquentActionRunRepository` rather than implement the interface from scratch — it
remains `@internal` and not a public extension point.

### 6. `RunCompensationJob` now takes its own queue lock

Compensations previously had no `WithoutOverlapping` protection at all (actions and parallel steps
always did). A new `locks.compensation_ttl_seconds` (default 900, independent of
`locks.action_ttl_seconds`) governs it; set `locks.enabled = false` to keep locking off entirely, as
before.

**If you published the config file before this release**, add the key to your `locks` array —
package config is merged only at the top level, so your own `locks` array is taken as it stands and
the new key is not filled in for you. Nothing breaks if you forget: a missing value inherits
`action_ttl_seconds`, and a zero or missing TTL can never produce a lock without an expiry.

### 7. New: `logging`

```php
'logging' => [
    'anomaly_level' => env('SAGA_LARA_FLOW_ANOMALY_LOG_LEVEL', 'info'),
    'channel' => env('SAGA_LARA_FLOW_LOG_CHANNEL'),
],
```

The first logging the package has ever done. `Runtime\AnomalyLog` is a second journal beside the
`flow_events` business history, and it covers exactly the things that are otherwise untraceable:

- `claim_lost` — a claim lost to whoever already owned the row.
- `outcome_rejected` — an outcome write refused because the row had changed hands.
- `batch_finished_early` — a parallel step completed after a duplicate delivery had already closed
  its batch.

All three are normal under at-least-once delivery and none of them fails a job, so without this
there would be nothing to investigate afterwards. Each line carries the run id, row id, sequence and
class. Set `anomaly_level` to `null` to silence it, or to `warning` if you want these surfaced by
your alerting. Writing the line is best-effort: an unusable level is treated as off, and a logger
that throws is swallowed rather than allowed to fail a job that was deliberately giving up quietly.

### 8. An unfinished compensation now stops a rollback

A compensation left `Pending` or `Running` when its level finishes — its worker died, or its job
never arrived — is treated the way a failed one is: under the `Stop` policy it halts the rollback,
and either way it is recorded as the run's secondary cause under `flow_run.exception['compensation']`.
Previously only `Failed` rows were looked for, so a rollback could finalize with a step silently
never undone and no trace of it anywhere. A compensation registered with the `Continue` policy still
does not stop the unwind.

### 9. Three scheduling methods take an `ActionSchedule`

The step's options used to be passed as a long tail of optional arguments. They are now carried by
`Data\ActionSchedule`, a readonly object with the same names as before:

```php
use DiscoveryUkraine\SagaLaraFlow\Data\ActionSchedule;

// before
$dispatcher->dispatch($run, $sequence, ChargeCard::class, [$orderId], true, false, $expiresAt);

// after
$dispatcher->dispatch($run, $sequence, new ActionSchedule(
    actionClass: ChargeCard::class,
    arguments: [$orderId],
    hasCompensation: true,
    expiresAt: $expiresAt,
));
```

Affected: `ActionDispatcher::dispatch()`, `ActionDispatcher::runInline()`, and
`ActionRecorder::scheduleAction()`. Nothing in the workflow API changes — this only matters if your
code calls those runtime classes directly.

### 10. `execute()` reports how far a step got

`ActionDispatcher::execute()`, `CompensationExecutor::execute()` and `ActionDispatcher::runInline()`
return `Enums\StepExecution` instead of `void`:

- **`Executed`** — the row was claimed, the step ran, and its outcome was recorded.
- **`ClaimLost`** — the row was already owned or no longer claimable; nothing ran.
- **`Superseded`** — the step ran, but the row changed hands before its outcome could be written.

The two failing cases look the same from a queued job, which returns quietly either way; call
`->settled()` if that is all you need. They differ where no competitor is supposed to exist: a
freshly created row that cannot be claimed is a broken invariant and still raises
`ActionClaimFailedException`, while being superseded afterwards is an ordinary race — the monitor and
the doctor act on the rows a sync run creates from their own processes — and replay resolves it from
whatever the row ended up holding.

### 11. Two more migrations: wait indexes, then one row per tag key

They ship as separate files because only the second one deletes anything. MySQL runs neither inside
a transaction, so both are written to be repeatable: whatever point a run dies at, running it again
carries on from there.

**`index_signal_waits`** supports the two new `FlowQuery` filters. `flow_signals` gains
`(status, name, flow_run_id)` and `action_runs` gains `(status, retry_signal, flow_run_id)`; the
shipped indexes on both tables lead with `flow_run_id`, which cannot serve a lookup by signal name
across runs.

**`unique_flow_tag_keys`** narrows `flow_tags` uniqueness from `(flow_run_id, key, value)` to
`(flow_run_id, key)`, the pair every tag writer matches on. The wider unique let two concurrent
first writes for one key insert two rows with different values.

> #### ⚠️ Rows are deleted if you have such duplicates
>
> For each `(flow_run_id, key)` the row with the highest `updated_at` is kept, the highest `id`
> breaking a tie, and the rest are removed. `flow_tags` timestamps are second-precision, so two
> writes inside one second fall back to insertion order. The rollback restores the old constraint
> but cannot bring deleted rows back.
>
> **Pause writes to `flow_tags` while this runs.** A write landing on an already-duplicated key
> between the winner being chosen and the losers being deleted is lost without a trace, and running
> the migration again will not bring it back. Keys that are not already duplicated are never touched.

Count the duplicates first — the table carries your configured `database.table_prefix`, `saga_` by
default:

```sql
-- MySQL
select flow_run_id, `key`, count(*) from saga_flow_tags group by flow_run_id, `key` having count(*) > 1;

-- PostgreSQL, SQLite
select flow_run_id, "key", count(*) from saga_flow_tags group by flow_run_id, "key" having count(*) > 1;
```

The narrower unique goes on before the wider one comes off, so the table is never left without one.
A tag written for a brand-new key while the migration sits between the cleanup and the constraint
can still make it fail; run it again.

If the count comes back empty there is nothing to lose and nothing to pause for — the migration adds
the constraint and stops.

Tag writing is otherwise unchanged: one value per key per run, last write wins. The int-to-string
normalisation now also sits on `Models\FlowTag` as a cast, so a `models.flow_tag` replacement should
extend that model rather than reimplement it.

### 12. A scalar workflow result is no longer wrapped

`flow_runs.result` is a JSON column, and a workflow returning anything other than an array had that
value stored as `{"value": ...}`. Nothing reversed the wrapper, so a parent calling
`$this->child(...)->run()` on a child returning an `int` got `['value' => 42]`, and a host reading
`$run->result` got the same.

The result is now written the way an action result already was — straight through the serializer,
with no envelope. A JSON column stores a bare scalar fine, and `Serializer::deserialize()` passes
non-arrays through untouched, so the child seam resolves the value the child actually returned.

Two things to check:

**Code that unwrapped by hand.** For a run completed on this release, `$run->result['value']` is now
just `$run->result`. Workflows returning an array are unaffected — those were never wrapped. Runs
completed before the upgrade are the caveat below.

**Rows completed before the upgrade keep their envelope, permanently.** They are neither migrated nor
unwrapped on read: a wrapped `42` and a workflow that legitimately returned `['value' => 42]` are
stored identically, so nothing automatic can tell them apart without corrupting the second case.

That is not a one-off. `$run->result` on a pre-upgrade run returns the wrapper for as long as the row
exists. And because a parent replays `handle()` from the top on every resume, a parent whose child
completed before the upgrade resolves that child to the wrapper on **every** replay, not only the
first one after the deploy.

So while pre-upgrade rows are still being read, code that consumes a scalar result has to accept both
shapes. Draining the affected parents before upgrading avoids the mixed period entirely. Note also
that this release does not rescue a parent already parked over such a child and written against the
unwrapped shape — that parent was failing before the upgrade and still fails after it.

### 13. A finished run no longer leaves live-looking steps and waits

`ActionStatus::Cancelled` was declared but never written. Cancelling, expiring or closing a run only
touched its `flow_runs` row, so a run that ended days ago kept steps reporting `pending`, `running`
or `awaiting_retry`, and wait-signals reporting `waiting`.

Every terminal transition now settles what the run was still holding: steps in those three statuses
become `cancelled`, and open wait-markers become `cancelled` too. Steps that had already reached an
outcome — `completed`, `failed`, `optional_failed`, `expired` — are untouched, so a cancelled run
still shows what actually ran. `SignalStatus::Cancelled` is new; a `match` over `SignalStatus` that
was exhaustive before now needs that arm.

Four things to check:

**Queries filtered by step or signal status.** A dashboard counting `action_runs` where
`status = 'running'` stops counting runs that ended. That was the point — those counts were wrong —
but a query written to compensate for it (joining `flow_runs` to exclude terminal runs) is now
redundant rather than harmful.

**`FlowQuery::whereAwaitingSignal()` and `whereAwaitingRetrySignal()`** stop matching a run once it
finishes, because it no longer holds the row they filter on. That is true of runs that finish on this
release; rows left behind by runs that finished earlier still carry `waiting` and `awaiting_retry`,
and their runs still match. Keep composing with `signalable()` while any of those remain — the SQL
below clears them if you would rather be done with it.

**A worker holding a `Running` step when its run is cancelled** races the settlement, and both
orders leave the row consistent. If the settlement lands first the step is `cancelled` and the
worker's outcome is refused, logged as `outcome_rejected` — the same fencing the monitor's expiry
already used. If the worker lands first the step keeps the outcome it earned and the settlement
passes over it. Expect `outcome_rejected` lines whenever runs are cancelled while steps are
genuinely in flight.

A refused outcome is not stored: before this release the late worker's result landed in
`action_runs.result`, and now the `outcome_rejected` line — the run, the step, its sequence and
class, and which outcome was refused — is what records it. If the step reaches an external system,
that log line is the pointer to reconcile from.

**Rows written before the upgrade keep their old status.** They are not migrated: the package's
migrations run automatically on `php artisan migrate`, and rewriting a host's history without being
asked is not something a migration should do. They are harmless — no sweep selects them any more (see
below) — but `saga-flow:show` will keep rendering them as they are. To settle them yourself:

```sql
UPDATE saga_action_runs
   SET status = 'cancelled'
 WHERE status IN ('pending', 'running', 'awaiting_retry')
   AND flow_run_id IN (
       SELECT id FROM saga_flow_runs
        WHERE status IN ('completed', 'failed', 'cancelled', 'expired')
   );

UPDATE saga_flow_signals
   SET status = 'cancelled'
 WHERE status = 'waiting'
   AND wait_sequence IS NOT NULL
   AND flow_run_id IN (
       SELECT id FROM saga_flow_runs
        WHERE status IN ('completed', 'failed', 'cancelled', 'expired')
   );
```

Adjust the table names if you set `database.table_prefix`.

### 14. No sweep selects work belonging to a finished run

Four scans gained the same condition: the doctor's `dueForRepair()` and `dueForStaleRunningRepair()`,
and the monitor's `dueForExpiration()` and `dueForTimeout()`.

Each of them rejected such a row anyway, but only *after* spending a slot on it and *before* any
counter could hold it off — the doctor returns ahead of its throttle, and the monitor has no counter
at all. Ordered oldest-first and never changing, an unsettled row sat at the head of every batch for
ever; enough of them and a pass filled up with dead rows and reached nothing live, while still
reporting success.

Item 13 stops new ones from appearing. This stops the ones already in your database from costing
anything, which is why no data migration is needed.

`FlowStatus::terminal()` is new, listing the four statuses a run never leaves. `isTerminal()` now
reads from it.

### 15. A run transition is refused if the row moved on

Every transition of a `flow_runs` row now writes conditionally, on the status the caller read before
deciding. If the row is no longer there, nothing is written and the transition raises
`ConcurrentFlowTransitionException`.

The hole it closes: nothing serialises an operator against a worker. The queue's per-run lock covers
jobs; it does not cover `saga-flow:cancel`, a host calling `FlowHandle::cancel()`, or the monitor's
sweep, which expires runs inline. Each side holds its own `FlowRun` instance, and whichever saved
last used to win outright — up to a worker writing `running` over a run an operator had already
cancelled, leaving a run that can never finish because its steps are settled and replay will not
resolve them.

Two things follow:

**The engine absorbs it; `FlowHandle` does not.** `FlowExecutor::drive()`, `FlowExecutor::expireRun()`
and the queued rollback's batch continuation catch it and stop: a job ends cleanly rather than
retrying, `runSync()` returns the run in the state the winner left it, and a parent awaiting the run
as a child reads its status and resolves accordingly. `FlowHandle::cancel()` and `compensate()`
deliberately let it through — see item 16. A rollback an operator asked for raises from its landing
too, for the same reason: nobody else is waiting on that answer. `CancelChildWorkflowJob` stops only
once the child it was sent to close is genuinely closed; losing it to something that leaves the child
live raises, so the close is retried rather than dropped.

**A same-state transition is checked too.** It used to return before touching the database, which is
precisely how a stale instance slipped past. It is now a conditional write like any other, and one
that has genuinely lost raises rather than reporting success. `cancelled_at` and `finished_at` are
still written once and never again, so a landing that repeats does not move the moment the run ended.

What this does **not** close: two workers that both legitimately see `running` both write
successfully, because the status does not change between them. That is ownership, not transition
ordering, and `WithoutOverlapping` on `run:{id}` remains what covers it.

Refused transitions are logged as `transition_lost`, with the status observed, the one intended, and
the one actually holding the row. A refusal leaves the `FlowRun` instance that asked for it exactly
as it was, so a caller that catches one can read the run's real status off its own model and act on
it — or simply try again.

### 16. `FlowHandle::cancel()` can now throw a second exception

`cancel()` and `compensate()` already raised `CannotCancelTerminalFlowException` when the run they
hold is terminal. They can now also raise `ConcurrentFlowTransitionException`, when the run was still
live as far as the handle knew but moved underneath it.

If you catch around either call, catch both. They stay distinct on purpose:
`CannotCancelTerminalFlowException` means the handle already knew the run was finished;
`ConcurrentFlowTransitionException` means it lost a race and the run is now in some other state,
which the message names.

Note that neither is a subclass of `InvalidTransitionException`, and `ConcurrentFlowTransitionException`
is deliberately not one either: that exception means the state graph forbids the move, and repeating
it cannot help, whereas a lost race is transient and re-reading the run is the sensible response.

### 17. `retryOnSignal()` takes a policy object and a `when:` predicate

The first parameter widens from `string` to `RetryPolicy|string`, and a fifth optional parameter,
`when:`, is appended. Every existing call site compiles and behaves identically — nothing is
required of you.

What is new is the fourth gate. A failure that passed `only:` is now also put to `when:` (or to the
policy's `shouldRetry()`), and a `false` there ends the policy for that failure: the step fails
exactly as it would have without one. With neither given, nothing is asked and nothing changes.

The one thing to know before writing a policy class: **nothing about it is persisted**, and only the
ceiling's stored value is frozen. `maxRetries()` is read when the step is scheduled and held on
`action_runs.retry_signal_max_attempts`, which is what replay enforces from then on — the same rule
`maxRetries:` has always followed. What is *not* frozen is the calling: your `handle()` builds the
policy again on every pass, and the builder re-reads `signal()`, `waitSeconds()` and `only()` off it
every time — including on a pass that only replays a step already scheduled or completed. Their
values take effect immediately. `shouldRetry()` is stored rather than called, and runs at the gate
only for a step that has failed. A deploy therefore changes all four for runs already in flight —
including which signal a parked step will next wait for.

`ActionBuilder` and `SagaStepBuilder` are constructed by the engine, and this release widens
`retryOnSignal()` on both. That is reachable if you override the public `Workflow::action()` to
return your own builder subclass: PHP refuses to load an override whose signature no longer matches,
so widen it to `RetryPolicy|string $signal` and accept the fifth `?Closure $when` parameter.

### 18. A run's relations read in a defined order

`FlowRun`'s relations no longer come back in whatever order the driver chose: `actions()`,
`compensations()`, `sideEffects()` and `children()` read by `sequence`, `events()` by
`recorded_at`, and `signals()` and `tags()` by `id`.

Nothing that was correct before changes. What to check is a read that appends its own order, or
one that pages by id — both now land behind the default, so clear it first:

```php
$run->signals()->reorder()->orderByDesc('id')->first();
$run->actions()->reorder()->chunkById(500, fn ($actions) => ...);
```

Eager loads, `withCount()`, `whereHas()` and plain `chunk()` need nothing. See
[Configuration](https://sagalaraflow.dev/configuration).

### 19. The table prefix is capped at 24 bytes

`database.table_prefix` rides along in every derived index name, and past a point the driver refuses
one outright (MySQL, at 7 characters) or truncates two onto each other (PostgreSQL, at 29). Six
indexes are now named explicitly rather than derived, which lifts the supported prefix to **24
bytes** — 24 characters of ASCII, fewer if you use anything else. The default `saga_` is five.

Nothing to do on a database that installed. One that failed part-way on a too-long prefix needs its
package tables dropped before `migrate` will get past the first one. The new names reach a fresh
install only, nothing reads them, and there is no rename migration.

### 20. A claim is verified after the transaction that made it commits

Claiming a step or an undo now reads the row back once its transaction has closed, and does not run
the body unless the claim is really there. A commit reporting success is not proof: on PostgreSQL a
failed statement aborts the transaction and turns the commit into a rollback, so a listener on
`ActionStarted` — or a model observer on the `flow_events` insert — that runs a failing query and
swallows it used to leave the step executing with nothing recording that it started.

Nothing to do unless your listeners swallow query failures. One that does now stops the step,
journalled as `claim_not_committed` and raised as `ActionClaimFailedException` in synchronous mode.
See [Reclaim & recovery](https://sagalaraflow.dev/reclaim-and-recovery).

### 21. `compensate()` plans the rollback without starting work, and stops if it cannot finish

Planning a manual rollback replays `handle()` to find the compensations. That replay no longer
settles a spent optional step (`action.optional_failed`, with its `OptionalActionFailed` event) and
no longer schedules a parallel block it had only reached to read. Nor is every throw the end of the
stack now: six classes end it, and an unexpected throw surfaces out of `compensate()`, run
untouched, rather than rolling back a truncated plan and reporting it complete.

Nothing to do unless a listener expected `OptionalActionFailed` during `compensate()`. The expiry
sweep absorbs such a throw instead, journalling `expiry_failed` and stepping over the run. See
[Sagas & compensation](https://sagalaraflow.dev/sagas-and-compensation).

### 22. Writes to a step check the row is still the one they read

Five recorder transitions — a give-up, a park, a rewind, settling a parked step, and the queue's
exhausted-attempts flag — now state the row they expect and the run's liveness in one `UPDATE`. One
that loses writes nothing, journals `write_refused` with a `site`, and returns `false` where it used
to return `void`. The claim is fenced on the run too, and still reports `claim_lost` as before.

The other thing to check: those five no longer call `save()`, so they raise no Eloquent model events
— an observer on your `ActionRun` model stops seeing them. Three publish a package event instead,
and settling a parked step and recording the queue's exhausted attempts publish none, by design. See
[Reclaim & recovery](https://sagalaraflow.dev/reclaim-and-recovery).

### 23. New: a refused outcome hands its payload to a listener

A worker that finishes a step whose row has moved on has its outcome refused, and until now what it
produced went nowhere. `ActionOutcomeRejected` and `CompensationOutcomeRejected` now carry it: the
value the step returned in the form the row would have stored, or the throw the engine deliberately
does not rethrow. Nothing is stored and nothing about the run changes. A listener that throws is
journalled as `rejection_undelivered` rather than failing the job, since this path must stay quiet.
See [Events](https://sagalaraflow.dev/events#refused-outcome).

### 24. A run the sweep cannot expire now steps aside instead of holding the page

The monitor reads one page of overdue runs, oldest first, and a run whose rollback it cannot plan
kept its place in it — enough of them at the head and the runs behind were never inspected. Such a
run is now held off for a widening window (`monitor.expiration.backoff`, 60s doubling to an hour)
and counted in two new `flow_runs` columns, so the page moves on and the journal stops repeating
itself. There is no attempt cap: the cause can be temporary. Run `php artisan migrate`.

### 25. A rollback no longer stops at a child that already finished

Planning a rollback treated any awaited child that was not `Completed` as the live frontier, so a
parent that carried on past a failed one under `->continueParentOnFailure()` rolled back only the
steps before it — and `compensate()` reported that partial unwind as complete. A terminal child is
now read the way an ordinary replay reads it, and the two throws that follow from it end the plan
like a recorded step failure does. Rollbacks that were silently short are now whole, so a
compensation you never saw run may start running. See
[Child workflows](https://sagalaraflow.dev/child-workflows).

### Recommended while you are here

- **Decide how a killed worker should be recovered** — see item 1. Leaving both reclaim and the
  monitor off means such a run waits forever, which is safe but needs a human.
- Read the [reclaim guide](https://sagalaraflow.dev/reclaim-and-recovery) before enabling reclaim in
  production: it changes which `Running` rows a redelivered job, or the doctor, may re-execute.
- If you already turned `repair.enabled` on, note that its new R3 rule
  (`repair.redispatch_stale_running_actions`, default `true`) acts on any row carrying a reclaim
  window — including one enabled per step via `reclaimStaleAfter()` while
  `actions.reclaim.stale_running.enabled` is globally `false`, since a per-step override outranks the
  global switch everywhere else too.

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
