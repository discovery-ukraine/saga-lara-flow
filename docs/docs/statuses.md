---
id: statuses
title: Statuses
sidebar_position: 21.5
---

# Statuses

Every row the engine writes carries a status from a backed enum, so a host can filter on it,
`match` on it, and read it straight out of the database.

## `FlowStatus` — the run

| Case | Meaning |
|---|---|
| `Pending` | Created, not yet driven. |
| `Running` | A replay pass is in progress. |
| `Waiting` | Suspended on something durable: a queued action, an open signal wait, or a child. |
| `Cancelling` | Rolling back. Not terminal — its compensations are still running. |
| `Completed` | `handle()` returned. |
| `Failed` | A business exception ended it, after whatever rollback its policy asked for. |
| `Cancelled` | Stopped because it was no longer wanted: `cancel()`, `compensate()`, or a child closed under `ChildClosePolicy::Cancel`. |
| `Expired` | Its own `expires_at` passed and the deadline was enforced. |

`Completed`, `Failed`, `Cancelled` and `Expired` are terminal: a run never leaves them, and it
refuses signals and cancellation once there.

`Cancelling` is not terminal, but no **step** starts under it. A rollback plans the stack it will
undo once, so a step that began afterwards would finish outside that plan — its compensation in no
stack, never run, under a run reporting a complete unwind. A job already queued for such a step is
refused when it tries to claim the row, a signal-gated retry will not start another cycle, and the
[doctor](./expiration-and-monitoring.md#repair-the-doctor) sends no replacement. Settling what
already started is a different question and carries on as usual: a step past its own deadline is
still expired, and the rollback's own compensations still run.

## `ActionStatus` — one step

| Case | Meaning |
|---|---|
| `Pending` | Scheduled. Its job has not settled it yet. |
| `Running` | Claimed by a worker that is executing it. |
| `Completed` | Returned a result, which replay hands back on every later pass. |
| `Failed` | Threw. Replay surfaces it as a business error. |
| `AwaitingRetry` | Failed and parked by [`retryOnSignal()`](./retry-on-signal.md), waiting for its signal. |
| `OptionalFailed` | An [optional](./optional-actions.md) step gave up; the seam returns its fallback. |
| `Expired` | The monitor enforced **this step's** own `expires_at`. |
| `Cancelled` | The run it belongs to finished before the step reached an outcome of its own. |

The difference between `Expired` and `Cancelled` is whose deadline ran out. `Expired` always
carries the step's own `exception` and an `action.expired` event; `Cancelled` carries neither,
because nothing happened to the step — the run under it ended.

## `SignalStatus` — one signal row

| Case | Meaning |
|---|---|
| `Waiting` | An open wait-marker: the run is parked at this sequence until the signal arrives. |
| `Received` | Delivered, not yet taken by a replay pass. |
| `Consumed` | Replay read its payload. |
| `TimedOut` | The monitor enforced the wait's `timeout_at`; replay surfaces that as a business error. |
| `Cancelled` | The run finished while the wait was still open. |

A delivered signal that no `awaitSignal()` ever matched keeps its `Received` status for good — it
records that a signal arrived and nobody used it.

## What a finished run leaves behind

Reaching a terminal state settles the work the run was still holding, so nothing under it keeps
reading as live:

- steps in `Pending`, `Running` or `AwaitingRetry` become `Cancelled`;
- open wait-markers (`Waiting`) become `Cancelled`.

Steps that already reached an outcome are untouched — a cancelled run still shows which of its
steps ran, and what each of them did.

One thing is deliberately left unsettled: a compensation still in `Pending` or `Running` when the
rollback stopped. That is not leftover state but an open operational item — a run that did not
fully unwind — and it is recorded on the run's own `exception` as
`CompensationUnfinishedException`. See [sagas & compensations](./sagas-and-compensation.md).

A step that is `Cancelled` is inert: a late job cannot claim it, and no monitor or repair sweep
selects it. A step under a run that is `Cancelling` is inert in the narrower sense above — no job
claims it and no repair sends another — while the deadline sweeps still settle it.

## `CompensationStatus` and `ChildStatus`

`CompensationStatus` (`Pending`, `Running`, `Completed`, `Failed`) tracks one compensation of a
rollback. `ChildStatus` (`Pending`, `Running`, `Completed`, `Failed`, `Cancelled`) tracks the link
between a parent and a [child workflow](./child-workflows.md) — the child's own run row carries a
`FlowStatus` of its own.
