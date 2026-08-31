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

## Repair (the doctor)

Separate from expiration: the **doctor** recovers a run whose progress was lost to a *dropped job* — an action that
never ran, a resume that never fired — rather than one that hit a deadline. It only ever re-dispatches existing jobs or
re-wakes flows (replay decides the rest); it never creates duplicate work.

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
  alone (re-drive it by hand with `saga-flow:kick`).
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
- **`queue_looping`** — drive the repair pass off the queue worker's idle loop instead of cron (same idea as
  `monitor.queue_looping`). When `enabled`, the pass runs at most once per `throttle_seconds`.

The doctor only ever re-dispatches existing jobs or re-wakes flows — replay decides the rest, so it never creates
duplicate work or mutates a business result.

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

## Pruning

Delete old terminal runs and their related rows:

```bash
php artisan saga-flow:prune --days=90
php artisan saga-flow:prune --before=2026-01-01 --dry-run
```

The default retention window is `prune.retention_days`.
