# Changelog

All notable changes to `saga-lara-flow` will be documented in this file.

## v1.2.0 - 2026-09-01

> ### ⚠️ Run `php artisan migrate` immediately after upgrading

Four migrations ship with this release — `add_reclaim_stale_running_columns`,
`index_signal_waits`, `unique_flow_tag_keys` and `add_expiry_backoff_to_flow_runs` — and the
engine writes their columns from the moment it starts.

```bash
composer update discovery-ukraine/saga-lara-flow
php artisan migrate

```
Deploy them together. They run from the package — do **not** `vendor:publish` them, or `migrate`
would try both copies.

**`unique_flow_tag_keys` deletes rows.** `flow_tags` uniqueness narrows from
`(flow_run_id, key, value)` to `(flow_run_id, key)`: for each pair the row with the highest
`updated_at` is kept — highest `id` breaking a tie — and the rest are deleted. Rolling the
migration back restores the old constraint but cannot bring rows back. Count yours first — the
query is in
[UPGRADING.md](https://github.com/discovery-ukraine/saga-lara-flow/blob/main/UPGRADING.md).

The four sections that follow share one shape: the engine reported a state the run was not in, and
nothing surfaced the difference. A rollback that came back short and called itself complete; a step
that executed with no record that it had ever started; a step that ran under a run already being
undone. Three of the defects below were found by running the suite on PostgreSQL for the first
time.

### Fixed: a rollback that comes back short now says so (#35, #48)

`FlowHandle::compensate()` plans a rollback by replaying `handle()` in a collecting mode, and two
seams let that plan end early while reporting it complete.

`collectCompensations()` decided the stack was finished by catching `Throwable`, so a throw from
anywhere in `handle()` — a builder argument, a workflow helper, a policy getter — truncated the plan
at that point and the run still landed in `Cancelled` (#35). And a child that had already reached
`Failed` or `Cancelled` was read as a live frontier, so a parent that carried on past one under
`->continueParentOnFailure()` rolled back only the steps before it, and a compensation you never saw
run stayed unapplied (#48).

Six exception classes now end a plan. Any other throw surfaces out of `compensate()` with the run
untouched, rather than rolling back a truncated plan and reporting it complete — the expiry sweep
absorbs the same throw instead, journalling `expiry_failed`. A recorded child resolves during
planning the way an ordinary replay resolves it. The planning replay also stopped writing on its way
through: it no longer settles a spent optional step, and no longer dispatches a parallel block it
had only reached in order to read.

### Fixed: no step starts on a run that has ended or is rolling back (#13, #14, #30, #37, #53)

Nothing between a run reaching a terminal state and a step executing asked whether the run was still
alive. The claim looked only at the step's own row, and `retryAction()` could rewind a settled step
back to `Pending` (#30). Parking a step for a signal-gated retry wrote a live `AwaitingRetry` step
and an open wait onto a run that had already been cancelled, where nothing would ever settle either
(#37). And a run in `Cancelling` — the state a rollback holds it in — still let a queued step be
claimed and completed after the stack to undo had been planned, so that step was never compensated
(#53).

Claiming a step is now one atomic conditional write that folds in the run's own status (#13). A
recorded outcome is never demoted, and five more recorder transitions write conditionally — a
give-up, a park, a rewind, settling a parked step, and the queue's exhausted-attempts flag; one
that loses writes nothing, journals `write_refused` and returns `false`. A finished run settles
what it was holding: steps in `pending`, `running` or `awaiting_retry` become `cancelled`, as do
open wait-markers (#14). Under `Cancelling`, a queued step, a doctor redispatch and a
signal-gated retry are all refused; settling a row that had already started is unchanged.

### Fixed: a step could execute on PostgreSQL with no record that it started (#41)

`startAction()` writes the claim, the `action.started` row and the `ActionStarted` event in one
transaction, so that a listener which throws rolls the claim back. On PostgreSQL a listener that
runs a failing query and swallows it aborts the transaction instead: `COMMIT` becomes a rollback and
reports success, `startAction()` returned `true`, and the step's body ran against a row still
`pending` with `attempts = 0` and no `action.started` event. A replay would run it again.

The claim is now verified after the transaction closes, and returns `false` — journalled as
`claim_not_committed` — when it did not survive; `CompensationRecorder::startCompensation()` gets
the same check. An Eloquent observer reaches the same window with no listener in sight, which is
why the check is on the claim rather than on where the event fires. The PostgreSQL skip in
`TransactionIntegrityTest` is gone.

### Fixed: one run the sweep cannot expire no longer blocks the ones behind it (#52)

`FlowMonitor::expireRuns()` reads one page of overdue runs, oldest first, and a run whose rollback
it could not plan stayed overdue at the head of that page. Healthy overdue runs behind it were never
inspected, on any sweep, however many ran — and the sweep reported `runs: 0`, which is also what an
idle sweep reports.

Such a run now steps aside for a widening window — `monitor.expiration.backoff`, 60 seconds doubling
to an hour — counted in two new columns of its own, and the healthy run behind it expires on the
next sweep. There is no attempt cap: the cause can be temporary, and the ceiling is what keeps a
permanent one cheap.

### Also fixed

- **A run's relations read in a defined order (#44).** `actions()`, `compensations()`,
  `sideEffects()` and `children()` by `sequence`, `events()` by `recorded_at`, `signals()` and
  `tags()` by `id`. PostgreSQL returns rows in physical order and an updated row moves to the end of
  the heap, so indexing into an unordered relation could hand back a different step than the one
  meant. A read that appends its own order, or pages by id, now lands behind the default and needs
  `reorder()`.
- **The `database.table_prefix` ceiling is stated rather than discovered (#42).** Derived index
  names carry the prefix; MySQL refuses an identifier past 64 characters and PostgreSQL truncates at
  63 bytes onto a name the same table already has. The measured ceiling was **7** characters on
  MySQL, against a default prefix of five, and either failure lands part-way through the initial
  migration. Six indexes are now named explicitly, which lifts it to **24 bytes** — and the suite
  installs the whole schema at exactly that, on all three drivers.
- **A scalar workflow result is no longer wrapped (#25).** A result that serialized to a scalar was
  stored as `{"value": ...}` with nothing to reverse it, so `$this->child(...)->run()` resolved the
  envelope rather than what the child returned. Rows completed before the upgrade keep theirs, and a
  parent replaying over such a child reads the wrapper every time — the UPGRADING item covers the
  mixed period.

### Added

- **`RetryPolicy` (#16)** — `retryOnSignal()` accepts a policy object, and a `when:` predicate
  decides whether *this* failure is worth parking. Every existing call site behaves identically.
  [Retry on signal](https://sagalaraflow.dev/retry-on-signal)
- **`ActionOutcomeRejected` and `CompensationOutcomeRejected` (#31)** — a worker that finishes a
  step whose row has moved on has its write refused, and the payload was dropped with the model.
  The events carry what the work produced: the recorded form of the result, or the throw the engine
  does not rethrow. Nothing is stored and nothing about the run changes, so where it goes is yours
  to decide. [Events](https://sagalaraflow.dev/events#refused-outcome)
- **`reclaim`** (`actions.reclaim.stale_running`, `sagas.reclaim.stale_running`) — a `Running` row
  becomes claimable again once its window has passed, which is how a worker killed mid-execution is
  recognised. Off by default; per-step via `reclaimStaleAfter()` and `enableStaleReclaim()`, and per
  compensation via `reclaimCompensationStaleAfter()` and `enableCompensationStaleReclaim()`. The
  doctor gained a matching rule for sequential actions
  (`repair.redispatch_stale_running_actions`); a parallel action or a compensation is bound to its
  batch and is recovered only by that job being redelivered.
- **`logging`** — `anomaly_level` (default `info`, `null` to silence) and `channel`. The package's
  first logging: a second journal beside `flow_events` for the refusals nothing else records.
  Writing is best-effort and never fails a job.
  [Reclaim & recovery](https://sagalaraflow.dev/reclaim-and-recovery)
- **Two `FlowQuery` wait filters and `FlowHandle::tag()` / `withTags()` (#15)**, with the
  `index_signal_waits` indexes that serve them. The tag uniqueness change in the banner arrives with
  them: it is what makes writing a tag an update rather than a second row.
- **`CompensationStepStarted`** marks one compensation beginning, distinct from
  `CompensationStarted`, which marks a whole rollback once per run. `RunCompensationJob` also takes
  its own queue lock, which compensations previously had none of.
- **`FlowStatus::terminal()`** lists the four statuses a run never leaves, and `isTerminal()` reads
  from it.

### The suite runs on MySQL and PostgreSQL (#33, #39)

CI ran on SQLite alone. The engine fences with conditional `UPDATE`s — the one form every supported
driver performs atomically, `lockForUpdate()` compiling to nothing on SQLite — and SQLite is the
driver that says least about whether they hold. The suite now also runs on MySQL, where a
conditional `UPDATE` reports *changed* rows rather than found ones, and on PostgreSQL, where a
failed statement poisons the transaction and `COMMIT` reports a rollback as success. The first
PostgreSQL runs are what turned up #41, #42 and #44 above.

### Behaviour changes

They are in
[UPGRADING.md](https://github.com/discovery-ukraine/saga-lara-flow/blob/main/UPGRADING.md), split
into **Action required** and **Behaviour changed** by whether they ask anything of you; every item
under the first carries a likelihood marker. Two that are easy to miss:

- **If you observe the package's Eloquent models** — every status write is now a conditional
  `UPDATE`, so an observer on a swapped-in `models.flow_run`, `models.action_run` or
  `models.compensation_run` no longer sees `updating`/`updated` for a transition. Most publish a
  package event and a `flow_events` entry instead; four write no replacement at all, by design.
- **If you `match` over the status enums** — `ActionStatus::Cancelled` is now written and
  `SignalStatus::Cancelled` is new, so a `match` that was exhaustive needs the new arm.

### Recommended

- **Decide how a killed worker's step is recovered.** A row that is already `Running` is not picked
  up by a redelivered job, so a worker killed mid-execution leaves a step nothing brings back —
  replay parks the run, and `saga-flow:kick` does the same. Turn on `reclaim` (the step runs again),
  or set `monitor.expiration.defaults.action` and schedule `saga-flow:monitor` (the step is marked
  `Expired`), or accept that such a run waits for a human.
- **Do not drive the engine inside a transaction of your own.** Rolling one back discards the
  engine's records while the work those records describe has already happened, and a second
  `compensate()` then runs the same compensations again. The engine cannot detect this from the
  inside — its own `transaction()` is a savepoint in there — so the boundary is documented instead,
  on [Queues, locks & idempotency](https://sagalaraflow.dev/queues-locks-idempotency) and in the
  README.
- **Turn `repair.enabled` on in production.** Still off by default, and still what recovers a step
  whose queue job was lost to a dying process.

### Still open

Named here so this release does not read wider than it is. Each carries its measurement in the
issue:

- **Under a rolling-back run**, the fence above covers a queued step, a doctor redispatch and a
  signal-gated retry. A replay that outlived the rollback still starts child workflows and runs side
  effects (#65), a signal is still accepted and can then never be consumed (#63), and a stale resume
  can re-run a whole rollback so its compensations execute twice (#64).
- **A rollback can still come back short** in narrower ways: a step that completes between the plan
  and the transition (#62), a plan built from a lagging read replica (#60), and an `Expired` child,
  which is still parked on as though it were in flight (#59).
- **The planning replay still writes the run's tags**, deliberately: a workflow branching on its own
  tag would otherwise plan a different stack from the one it built (#50).
- **A conditional write fences against a cancellation that has committed**, not one still in flight
  (#54).
- **An action the doctor has given up on has no operator recovery.** `saga-flow:kick` re-wakes the
  run but resets no counter, so the step stays out of reach until a retry cycle reschedules it
  (#67).

Thanks to @alex543644 for #15, and for the wait filters and tag writers that closed it.

**Full Changelog**: https://github.com/discovery-ukraine/saga-lara-flow/compare/v1.1.1...v1.2.0

## v1.1.1 - 2026-08-24

### Documentation: deadlines and the expiration sweep

No engine changes and **no migration** — this release says out loud something the documentation only
ever implied.

Deadlines (`timeoutAfter()`, `expiresAt()`, `#[FlowTimeout]`, `monitor.expiration.defaults`) are
enforced by the expiration sweep, not by timers. If neither `saga-flow:monitor` nor the
queue-looping listener is running, **no deadline is ever enforced** — `queue:work` on its own does
not check them, and neither does delivering a signal.

- `signals.md` and `actions.md` now say so where the deadline APIs are introduced, instead of only
  on the expiration page.
- `installation.md` gained a **Schedule the monitor** step.
- `testing.md` gained a **Testing deadlines** section: `travel()` alone changes nothing, and the
  sweep only marks the wait — a second queue drain is needed before the workflow has replayed and
  reacted.
- `expiration-and-monitoring.md` gained **Deadlines are approximate**: because the sweep is the only
  writer of "this deadline passed", a signal delivered after its deadline but before the next sweep
  is still accepted.

Two tests now cover that scenario so it cannot regress silently.

Making deadlines strict — enforcing them inline, at the seams that already hold the row — is
proposed as an opt-in for 1.2 in #19.

Thanks to @alex543644 for reporting #17.

## v1.1.0 - 2026-08-22

> ### ⚠️ Run `php artisan migrate` immediately after upgrading

This release adds a migration, and it is **not optional**. Every action the engine schedules —
with or without a retry policy — writes the new columns. An application that upgrades the package
without running its migrations will break **ordinary workflow execution** with an unknown-column
error on the very next action it schedules.

```bash
composer update discovery-ukraine/saga-lara-flow
php artisan migrate



```
Deploy the two together. See [UPGRADING.md](https://github.com/discovery-ukraine/saga-lara-flow/blob/main/UPGRADING.md).

#### Added: retry a failed saga step on a signal (#9)

A failing step used to take the whole run with it — the saga rolled back and completed work was
undone. That is right for a bug, wrong for a step that failed because the world was not ready yet: a
declined card, a service still provisioning. `retryOnSignal()` parks such a step instead, and an
external signal re-runs **that step alone**.

```php
$this->action(ChargeCard::class, $orderId)
    ->compensateWith(RefundCard::class, $orderId)
    ->retryOnSignal(
        'balance-refilled',
        maxRetries: 3,                                // null = unbounded
        waitSeconds: 86400,                           // how long one wait may last
        only: [InsufficientBalanceException::class],  // null = park on any exception
    )
    ->run();



```
Deliver `balance-refilled` the way you deliver any other signal and `ChargeCard` runs again, alone;
earlier steps stay completed and un-compensated. `saga()->step()` mirrors the method.

The failure layers stack: Laravel's `$tries` first, then `retryOnSignal()`, then
`continueOnFailure()`, then hard failure and compensation. When the budget is spent or the wait times
out, the step fails exactly as it would have without the policy — same `ActionFailedException`
carrying the last attempt's message, same rollback. There is no new exception class to catch.

A retry consumes **no new sequence**: the step reuses its own ordinal and `action_runs` row, and the
waiting consumes no ordinal at all, so downstream steps land identically whether it retried or not.

**Operator surface.** `saga-flow:show` gains a **Retry** column (signal, spent budget, live wait
deadline); `saga-flow:list` annotates a parked run with the signal it needs. Two new events,
`ActionAwaitingRetry` and `ActionRetried`.

See the [Retry on signal](https://sagalaraflow.dev/retry-on-signal) guide.

#### Added: batch `tags()` inside a workflow (#8)

Tagging from inside `handle()` only allowed one key at a time, while the same concept already had a
bulk form at creation (`->withTags([...])`) and declaratively (repeatable `#[Tag]`). Both `tag()` and
`tags()` are now fluent:

```php
$this->tags([
    'priority' => 'high',
    'attempt'  => 2,      // int values are cast to string
    'orders'   => null,   // a tag with no value
]);



```
#### Behaviour changes

Two, both detailed in [UPGRADING.md](https://github.com/discovery-ukraine/saga-lara-flow/blob/main/UPGRADING.md):

- **`SignalRepository` gained two methods** (`earliestPendingSince`, `latestForSequence`). Shipped as
  a minor because the repository contracts are not a public extension point — they are now marked
  `@internal`, and `config('saga-lara-flow.models.*')` remains the supported swap point.
- **Delivering into an open wait, and timing a wait out, no longer raise Eloquent model events.**
  Both are now single conditional `UPDATE`s — the only form that is atomic on every supported driver
  (`lockForUpdate()` compiles to nothing on SQLite), and what stops a delivery and a timeout from
  both claiming the same wait. An observer on a swapped-in `models.flow_signal` no longer sees them;
  `flow_events` and the package's own Laravel events still record every transition.

#### Recommended

- **Turn `repair.enabled` on in production.** The doctor is what recovers a step whose queue job was
  lost to a dying process. It is off by default.
- **Check your package-event listeners.** A synchronous listener that throws interrupts the engine's
  replay and can fail a healthy run. Mark them `ShouldQueue`, or make sure they cannot throw.

**Full Changelog**: https://github.com/discovery-ukraine/saga-lara-flow/compare/v1.0.5...v1.1.0

## v1.0.5 - 2026-07-05

### Added

- `FlowQuery::active()` (alias `FlowQuery::signalable()`) — a scope for runs that can still receive a
  signal: `Pending`, `Running`, or `Waiting`. Backed by `FlowStatus::signalable()` as the single
  source of truth, which mirrors the delivery boundary `SignalDispatcher` accepts (non-terminal),
  minus `Cancelling`. Use it to locate a run to deliver a signal to. Previously the only way was
  `->whereStatus(FlowStatus::Running, FlowStatus::Waiting)`, and reaching for `->running()` alone
  silently missed the target: a flow parked on `awaitSignal()` sits in `Waiting`, not `Running`.

### Documentation

- Clarified delivering a signal — how to find the run by workflow + tag without a `$runId` (using
  `signalable()`), that a signal reaches any non-terminal run (not only `Running`), and that
  `signalIfRunning()` means "unless the run has already finished", not "only if status is `Running`".

## v1.0.4 - 2026-07-04

### Documentation

- Clarified how to react to a failed step. `try/catch` around `->run()` inside `handle()` **does**
  catch `ActionFailedException` / `FlowExpiredException` (and `ChildWorkflowFailedException`) — but on
  the **replay** pass, not the instant the action fails: in queued mode the action runs off the
  `handle()` stack in its own job, and the failure surfaces only when the engine re-drives `handle()`.
  In sync mode `run()` re-throws the action's raw exception instead. Documented that `try/catch` is
  for **local** branching, while the `FlowFailed` event is the recommended hook for cross-cutting
  failure reporting (it fires once on the terminal transition, on both the direct-fail and
  fail-after-compensation paths, sync or queued), and that reporting from inside `handle()` must
  re-throw so the run still fails and compensates. Touches the Actions, Child workflows, and Events
  docs plus the README. No code changes.

## v1.0.3 - 2026-07-02

### Fixed

- The migration now ships with a timestamp-prefixed filename
  (`2026_07_02_000000_create_saga_lara_flow_initial_tables`), matching how first-party Laravel
  packages (Sanctum, Cashier, Telescope) name their auto-loaded migrations. In 1.0.2 it loaded under
  a bare, dateless name (`create_saga_lara_flow_initial_tables`), which looked wrong in
  `migrate:status` and sorted unpredictably against the app's own migrations.

### Upgrading from 1.0.1 / 1.0.2

The migration's recorded name changes, so a host app that already ran 1.0.1 or 1.0.2 will see the
new name as "pending" and `php artisan migrate` would try to create the tables again. The engine
tables already exist, so either:

- **Fresh dev database:** `php artisan migrate:fresh` (destroys data — dev only), or
- **Keep your data:** rename the recorded migration so Laravel treats it as already run —
  `UPDATE migrations SET migration = '2026_07_02_000000_create_saga_lara_flow_initial_tables' WHERE migration = 'create_saga_lara_flow_initial_tables';`

Brand-new installs (no prior 1.0.1/1.0.2) are unaffected — `composer require` + `php artisan migrate`.

## v1.0.2 - 2026-07-02

### Fixed

- **`runsMigrations()` did not actually load the migration in 1.0.1.** The migration shipped as a
  `.php.stub`, but Laravel's migrator only treats a registered path as a migration file when it ends
  in `.php` (`Migrator::getMigrationFiles`); a `.php.stub` path was globbed as if it were a directory,
  matched nothing, and was **silently skipped** — so `php artisan migrate` (and `migrate:status`)
  never saw the engine's tables. The migration is now a real `.php` file and runs as intended with a
  plain `php artisan migrate`, no publish step.

### Changed

- Publishing the migration is **no longer supported** as a customization path: because the migration
  auto-runs from the package, a published (timestamped) copy would run alongside it and collide.
  Customize the schema through config instead — `database.table_prefix`, `database.connection`, and
  the swappable `models.*`.

## v1.0.1 - 2026-07-01

### Changed

- The engine's migration now loads directly from the package (`runsMigrations()`), so a host app
  installs with just `php artisan migrate` — the `vendor:publish --tag="saga-lara-flow-migrations"`
  step is no longer required. Future package migrations are picked up the same way, via
  `composer update` + `php artisan migrate`. Publishing the migration is still supported for anyone
  who wants to customize the schema (a published copy overrides the package's own).

## v1.0.0 - 2026-07-01

**Saga Lara Flow 1.0.0** — the first stable release.

A workflow management engine with an integrated **Saga pattern**, built on top of Laravel Queues.
Write a long-running, durable business process as a single deterministic PHP method: each step runs,
is recorded, and survives worker restarts through exception-based suspension and replay. When a step
fails partway through, registered compensations roll back the completed work in reverse order.

### Highlights

- **Deterministic workflows** via exception-based suspension and replay, keyed by `(flow_run_id, sequence)`.
- **Actions** with native Laravel DI, retries (`$tries`), timeouts (`$timeout`), and per-step deadlines.
- **Saga pattern**: action-level and grouped compensations with configurable failure policies, sequential or parallel
  rollback.
- **Signals** with optional timeouts; **side effects** with record-and-replay.
- **Parallel action blocks** (`failFast` / `waitAllThenFail`) and **optional actions** with fallbacks.
- **Child workflows** with close policies (`Abandon` / `Cancel` / `Fail`).
- **Tags** and a fluent, type-safe `FlowQuery`.
- **Expiration monitoring** (scheduler or queue-looping) and a **repair/doctor** pass for dropped jobs.
- **Octane / multi-tenancy** safety via capture-at-creation and opt-in auto restore/revert hooks.
- **Configurable attributes** (`#[Flow]`, `#[FlowQueue]`, `#[FlowTimeout]`, `#[Tag]`, `#[Tenancy]`, …).
- **Artisan tooling**: `saga-flow:list|show|signal|cancel|kick|monitor|repair|prune`, `make:workflow`, `make:action`.

### Requirements

- PHP `^8.5`
- Laravel 13 (`illuminate/*: ^13`)

### Install

```bash
composer require discovery-ukraine/saga-lara-flow
php artisan vendor:publish --tag="saga-lara-flow-migrations"
php artisan migrate




```
### Links

- Documentation: https://sagalaraflow.dev
- Packagist: https://packagist.org/packages/discovery-ukraine/saga-lara-flow

**Full Changelog**: https://github.com/discovery-ukraine/saga-lara-flow/commits/v1.0.0
