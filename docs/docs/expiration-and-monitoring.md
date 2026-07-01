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

Applied at write time when no explicit deadline is set: `run` on create, `action` on schedule,
`signal` on await. `null` = off. There is no per-entity opt-out flag; to bypass a default for one
entity, pass an explicit (far-future) deadline.

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
    'enabled' => true,
    'grace_seconds' => 60,
    'max_attempts' => 10,
    'redispatch_actions' => true,
    'wake_waiting' => true,
],
```

Schedule it, or loop it off the worker:

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
