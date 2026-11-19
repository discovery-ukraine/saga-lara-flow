---
id: expiration-and-monitoring
title: Expiration & monitoring
sidebar_position: 14
---

# Expiration & monitoring

Runs, actions, and signal waits can carry deadlines — set explicitly (`->expiresAt(...)`,
`->timeoutAfter(...)`, `#[FlowTimeout]`) or via the configured defaults in
`monitor.expiration.defaults`. Something has to *notice* an expired deadline; there are two ways to drive the sweep.

## Default deadlines

```php
'monitor' => [
    'expiration' => [
        'defaults' => ['run' => 3600, 'action' => 600, 'signal' => 86400],
    ],
],
```

All three values are **in seconds** (here: 1 hour for a run, 10 minutes for an action, 24 hours for a signal wait). They
are applied at write time when no explicit deadline is set: `run` on create,
`action` on schedule, `signal` on await. `null` = off (no implicit deadline). There is no per-entity opt-out flag; to
bypass a default for one entity, pass an explicit (far-future) deadline.

The `signal` default also bounds a [retry-on-signal](./retry-on-signal.md) wait when the call site passes no
`waitSeconds:`. A step's own `action` deadline does not: an action deadline bounds *execution*, and a parked step is not
executing.

A child run takes the `run` default as a root run does, below its class's `#[FlowTimeout]` and an
explicit `->expiresAt()` on the child builder. It never takes its parent's deadline — see
[child workflows](./child-workflows.md#a-childs-own-class).

## Driving the sweep

### Scheduler (recommended)

```php
use Illuminate\Support\Facades\Schedule;

Schedule::command('saga-flow:monitor')->everyMinute();
```

### Queue looping (opt-in)

Drive the sweep off the queue worker's idle loop instead of cron:

```php
'monitor' => [
    'queue_looping' => ['enabled' => true, 'throttle_seconds' => 30],
],
```

Useful when you have always-on workers but no scheduler. The sweep is throttled so it runs at most once per
`throttle_seconds`. The listener is registered while the service provider boots, so the option has to be set in
configuration — flipping it at runtime has no effect.

**If neither is driving the sweep, no deadline in the package is ever enforced.** A run passes its
`expiresAt`, a step passes its own, a signal wait passes its `timeoutAfter` — and nothing happens.
`queue:work` on its own does not check deadlines, and neither does delivering a signal.

## Deadlines are approximate

The sweep is the only writer of "this deadline passed", which means a deadline is enforced no sooner than the next
sweep. With `everyMinute()` that is a window of up to a minute; with queue looping it is up to `throttle_seconds`.

The visible consequence: a signal delivered *after* its wait's deadline but *before* the next sweep is still accepted,
and the workflow carries on as though the wait succeeded. Once the sweep has marked the wait `timed_out`, the same
delivery arrives too late and the workflow sees
`AwaitSignalTimeoutException` instead.

This is deliberate. Keeping the sweep the single writer of a wait's status is what makes delivery, timeout and the retry
seam safe to run concurrently. If your deadline is a hard business boundary rather than a safety net, enforce it in the
workflow — the payload of a late signal can be checked against a deadline you captured with `sideEffect()`.

A sweep only ever looks at work belonging to a run that is still going. A run that has finished settles its own steps
and waits as it ends (see [statuses](./statuses.md)), and the scan skips whatever was left unsettled before that, so a
batch is always filled with candidates a sweep can actually act on.

A deadline is also enforced only once. The sweep moves a run it expires into `Cancelling`, and no pass is driven for a
run there, so a job queued before the sweep — a resume owed to a wait it then expired — ends without re-entering
expiration. The rollback the sweep planned is the only one, and each compensation on it runs once.

## A run the sweep cannot expire

Expiring a run means replaying it to find what to undo, and that replay can throw — a workflow reading something that
has since gone, or a deploy that edited a workflow with runs still in flight. The sweep journals `expiry_failed`,
leaves the run exactly where it found it, and holds it off for a while so the page of candidates moves on to the runs
behind it:

```php
'monitor' => [
    'expiration' => [
        'batch_size' => 100,
        'backoff' => ['base_seconds' => 60, 'max_seconds' => 3600],
    ],
],
```

The window doubles with each failure up to `max_seconds`, and `flow_runs.expiry_attempts` counts them for you to
query. A held-off run rejoins the queue on the time its window opens rather than on its original deadline, so however
many of them there are they cannot queue ahead of a run that has been overdue longer than they have waited. There is **no attempt cap**: the cause is often temporary, so a run that becomes plannable again is expired on
the next open window. Nothing resets the count, so a run that has been failing since Tuesday says so. Fixing the
workflow is still the actual remedy — see [Reclaim & recovery](./reclaim-and-recovery.md).

All of that concerns the plan drawn before the run is moved. Every run the sweep expires is then
moved into `Cancelling`, one with nothing to undo included, and the plan is drawn again: that second
one is what the rollback unwinds, and it holds the step whose owed attempt completed while the first
was being drawn. A second plan that came back without an ordinal the first had takes that ordinal
from it and journals `replan_incomplete`; a replay that throws journals `replan_failed` and leaves
the first plan standing. Neither is surfaced — the run has been taken, so the rollback goes ahead
rather than stopping in `Cancelling` with nothing to move it on.

A run with nothing to undo therefore passes through `Cancelling` on its way to `Expired`, and a step
that completes while its empty plan is being drawn is undone before the run lands.

## A rollback that stalls

The move into `Cancelling` writes down where the rollback lands and why: `flow_runs.rollback_target`
(`expired`, `failed` or `cancelled`) and `rollback_cause`, beside an owner token in
`rollback_token`. Whoever moved the run owns its rollback. If that owner stops — its process dies
after the move, or the job of a compensation is lost — nothing else would ever move the run on: no
pass is driven for a run in `Cancelling`, and the doctor sends no step under it a job.

A rollback nothing has moved for a grace window is taken over and carried to the end its intent
names. Nothing has moved it when the run itself has not been written since, no compensation was
registered, claimed or settled since, and no compensation is held `Running` by a worker still inside
its [reclaim](./reclaim-and-recovery.md) window. The sweep takes over a stalled expiry, and the
[doctor](#repair-the-doctor) everything else:

```php
'monitor' => [
    'expiration' => [
        'rollback_grace_seconds' => 60,
    ],
],
```

The takeover is a conditional write on the run that replaces the owner token the sweep read, so of
two sweeps one takes it, and a run taken over or landed since it was read is left alone. The plan is
drawn again under `Cancelling`, where no step can complete, and a compensation is registered once
per step: the one the earlier owner already ran is reused rather than run again, and one it left
`Pending` is sent a job. Every compensation job carries the owner that sent it, and its claim is
refused once the rollback has another, so a late job of the earlier owner — one queued behind a
backlog longer than the grace window — runs nothing, and an earlier owner that turns out to be alive
stops at its next claim or level and leaves the rest to the one that took over. A level whose
compensation a worker of the earlier owner claimed in the moment before the takeover is left as it
is until that row settles, and the rollback is taken over again once nothing has moved it for the
grace window.

The claim and the takeover are separate writes on separate rows. On PostgreSQL a claim that starts
in the same instant as a takeover can still read the earlier owner and run beside the one that took
over; the claim lets only one of them run that undo, but the other one's level can then close early.

A compensation `Running` with no reclaim deadline holds the rollback where it is for as long as the
row stays that way — the worker may still be running it. With `sagas.reclaim.stale_running` enabled,
the row becomes claimable once its deadline passes, and the rollback is taken over then.

A stalled rollback whose plan cannot be drawn again stays in `Cancelling` under its new owner and is
journalled as `rollback_resume_failed`.

Every takeover by the sweep, planned or not, holds the run off with the same window as an expiry it
cannot plan, doubling each time (`backoff`, counted in `expiry_attempts`). The grace window judges by
time alone, so a queue that keeps a job waiting longer than `rollback_grace_seconds` makes a healthy
rollback look stalled: it is taken over, and the jobs already queued for it claim nothing. The
growing window is what lets the latest owner's jobs through in the end. Set
`rollback_grace_seconds` above the longest your queue makes a job wait. A run that went into `Cancelling` before its intent was
written down carries none, and is left where it is.

## Repair (the doctor)

Separate from expiration: the **doctor** recovers a run whose progress was lost to a *dropped job* —
an action that never ran, a resume that never fired, a run that never started, a rollback that
stopped — rather than one that hit a deadline. For a run going forwards it only ever re-dispatches
existing jobs or re-wakes flows (replay decides the rest); it never creates duplicate work.

```php
'repair' => [
    'enabled' => false,
    'grace_seconds' => 60,
    'batch_size' => 100,
    'max_attempts' => 10,
    'backoff' => ['base_seconds' => 10, 'max_seconds' => 300],
    'redispatch_lost_actions' => true,
    'redispatch_stale_running_actions' => true,
    'wake_stuck_flows' => true,
    'redispatch_lost_starts' => true,
    'resume_stalled_rollbacks' => true,
    'queue_looping' => ['enabled' => false, 'throttle_seconds' => 60],
],
```

Every parameter:

- **`enabled`** — master switch. Off by default; the doctor never runs until you opt in.
- **`grace_seconds`** — minimum age, **in seconds**, before an entity is even *considered* stuck. This guards against
  racing a job that is simply still in flight: the doctor ignores anything younger than this, so a slow-but-alive action
  is left alone. Raise it if your jobs legitimately run long.
- **`batch_size`** — how many candidate entities one repair pass inspects at most. Only entities of runs that have not
  finished are counted against it.
- **`max_attempts`** — per-entity cap. After this many repair attempts the doctor gives up on that entity and leaves it
  alone. A kick refills that budget — held off for `grace_seconds` first, like any freshly dispatched row — so the cap
  holds the automatic pass off rather than ending the run's recovery.
- **`backoff`** — exponential backoff between repair attempts for a single entity, clamped between
  `base_seconds` and `max_seconds`. Prevents the doctor from hammering the same stuck entity.
- **`redispatch_lost_actions`** — enable R1: re-dispatch a lost queue job for a stuck sequential
  `Pending` action (an action whose `RunActionJob` never arrived).
- **`wake_stuck_flows`** — enable R2: re-wake a flow stuck in the `Waiting` status after a resume that never fired.
- **`redispatch_stale_running_actions`** — enable R3: re-dispatch a fresh job for a stuck sequential
  `Running` action past its own **reclaim** deadline (a worker that died mid-execution, rather than a job that never
  arrived). It acts on any row carrying such a deadline — set globally, or by a single step that opted itself in. With
  reclaim configured nowhere, no row carries one and the rule is inert. Parallel actions and compensations are out of
  scope for it. See
  [Reclaim & recovery](./reclaim-and-recovery.md).
- **`redispatch_lost_starts`** — enable R4: send the first job again for a run still `Pending` after the
  grace window — a root run, a child, or a child started again by a retry signal whose `RunWorkflowJob`
  never arrived. A run started with `runSync()` whose process died before it began is started on the
  queue. The doctor judges by time alone, so a run that is merely queued behind a backlog longer than
  `grace_seconds` gets a second job too. That job is a `RunWorkflowJob` like the first, so while
  [locks](./queues-locks-idempotency.md#locks) are enabled it waits for an original that is still being
  worked rather than driving the run beside it, and it drives the run only if it is still `Pending` when
  it gets there. So a copy whose worker dies once it has started the run is not driven again on
  redelivery: the run stays `Running`, where the doctor does not look, until `saga-flow:kick`. A child whose parent has closed, or is closing, is left to the parent's close policy
  unless that policy is `Abandon`. With the package on a [dedicated connection](./queues-locks-idempotency.md#host-transactions)
  the rule cannot tell a lost job from one your own open transaction still holds back.
- **`resume_stalled_rollbacks`** — enable R5: take over a rollback to `Failed` or `Cancelled` that
  nothing has moved for `grace_seconds`, and carry it to the end, as described in
  [A rollback that stalls](#a-rollback-that-stalls). A stalled expiry is the sweep's. Each takeover
  spends the run's repair budget, so a rollback whose jobs keep getting lost is given up on after
  `max_attempts`; one whose plan cannot be drawn again is journalled as `rollback_resume_failed`.
- **`queue_looping`** — drive the repair pass off the queue worker's idle loop instead of cron (same idea as
  `monitor.queue_looping`). When `enabled`, the pass runs at most once per `throttle_seconds`.

For a run going forwards the doctor only ever re-dispatches existing jobs or re-wakes flows — replay
decides the rest, so it never creates duplicate work or mutates a business result. It sends no step
of a run that is rolling back (`Cancelling`) a job: one would complete outside the stack that
rollback already planned. See [statuses](./statuses.md).

:::tip Turn it on in production
Any step whose job is committed and then dispatched can lose that job to a dying process — including a step restarted
by [retry on signal](./retry-on-signal.md), which then sits `Pending` with nothing behind it.
`redispatch_lost_actions` is exactly the recovery for that, and it does nothing until `repair.enabled` is `true`.
:::

Schedule it, or loop it off the worker (`repair.queue_looping.enabled`):

```php
Schedule::command('saga-flow:repair')->everyFiveMinutes();
```

To re-drive a single stuck run by hand:

```php
SagaFlow::kick($runId);          // or:
// php artisan saga-flow:kick {run}
```

A kick re-drives a run that may still start work. A rollback that has stalled — nothing has moved it
for `grace_seconds`, and no worker that may be alive holds one of its compensations — is taken over
and carried on under its own target and cause, as R5 does, without its throttle, and its repair
budget is refilled: the way on for a rollback the doctor gave up on, or one that stalled while the
doctor is off. A rollback still moving is left to its owner, as is one that undoes nothing (a child
closed without compensation), and a run that has finished is left exactly as it was; the command
reports the status instead of claiming a re-drive. A compensation a worker of the earlier owner
claims in the moment before the takeover holds the rollback where it is, as it does for R5. The
command warns when it still sees that compensation held; a rollback that does not move after a kick
needs another once nothing holds it.

A run that has not started yet gets its first job again, a `RunWorkflowJob` that drives it only from
`Pending`, for the reason given under `redispatch_lost_starts`; any other run gets a resume.

It also refills the repair budget of the run and of every step it has not finished, and sends a fresh job for the
sequential step the run is parked on — so a run stopped at a step the doctor gave up on moves again, rather than
replaying up to that step and parking on it a second time. The step it reaches is the one R1 and R3 read, without their
throttle: `Pending`, or `Running` past its [reclaim](./reclaim-and-recovery.md) deadline. A `Running` row still inside
that window belongs to a worker that may be alive and is left to it. A parallel block gets its budget back and nothing
else, for the same reason R1 and R3 leave batch-bound work alone.

## Pruning

Delete old terminal runs and their related rows:

```bash
php artisan saga-flow:prune --days=90
php artisan saga-flow:prune --before=2026-01-01 --dry-run
```

The default retention window is `prune.retention_days`.
