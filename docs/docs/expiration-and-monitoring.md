---
id: expiration-and-monitoring
title: Expiration & monitoring
sidebar_position: 13
---

# Expiration & monitoring

Runs, actions, and signal waits can carry deadlines — set explicitly (`->expiresAt(...)`,
`->timeoutAfter(...)`, `#[FlowTimeout]`) or via the configured defaults in
`monitor.expiration.defaults`. Something has to *notice* an expired deadline; there are two ways to
drive the sweep.

## Default deadlines

```php
'monitor' => [
    'expiration' => [
        'defaults' => ['run' => 3600, 'action' => 600, 'signal' => 86400],
    ],
],
```

All three values are **in seconds** (here: 1 hour for a run, 10 minutes for an action, 24 hours for a
signal wait). They are applied at write time when no explicit deadline is set: `run` on create,
`action` on schedule, `signal` on await. `null` = off (no implicit deadline). There is no per-entity
opt-out flag; to bypass a default for one entity, pass an explicit (far-future) deadline.

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

Useful when you have always-on workers but no scheduler. The sweep is throttled so it runs at most
once per `throttle_seconds`.

## Repair (the doctor)

Separate from expiration: the **doctor** recovers a run whose progress was lost to a *dropped job* —
an action that never ran, a resume that never fired — rather than one that hit a deadline. It only
ever re-dispatches existing jobs or re-wakes flows (replay decides the rest); it never creates
duplicate work.

```php
'repair' => [
    'enabled' => false,
    'grace_seconds' => 60,
    'batch_size' => 100,
    'max_attempts' => 10,
    'backoff' => ['base_seconds' => 10, 'max_seconds' => 300],
    'redispatch_lost_actions' => true,
    'wake_stuck_flows' => true,
    'queue_looping' => ['enabled' => false, 'throttle_seconds' => 60],
],
```

Every parameter:

- **`enabled`** — master switch. Off by default; the doctor never runs until you opt in.
- **`grace_seconds`** — minimum age, **in seconds**, before an entity is even *considered* stuck. This
  guards against racing a job that is simply still in flight: the doctor ignores anything younger than
  this, so a slow-but-alive action is left alone. Raise it if your jobs legitimately run long.
- **`batch_size`** — how many candidate entities one repair pass inspects at most.
- **`max_attempts`** — per-entity cap. After this many repair attempts the doctor gives up on that
  entity and leaves it alone (re-drive it by hand with `saga-flow:kick`).
- **`backoff`** — exponential backoff between repair attempts for a single entity, clamped between
  `base_seconds` and `max_seconds`. Prevents the doctor from hammering the same stuck entity.
- **`redispatch_lost_actions`** — enable R1: re-dispatch a lost queue job for a stuck sequential
  `Pending` action (an action whose `RunActionJob` never arrived).
- **`wake_stuck_flows`** — enable R2: re-wake a flow stuck in the `Waiting` status after a resume that
  never fired.
- **`queue_looping`** — drive the repair pass off the queue worker's idle loop instead of cron (same
  idea as `monitor.queue_looping`). When `enabled`, the pass runs at most once per `throttle_seconds`.

The doctor only ever re-dispatches existing jobs or re-wakes flows — replay decides the rest, so it
never creates duplicate work or mutates a business result.

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
