---
id: events
title: Events
sidebar_position: 21
---

# Events

The engine mirrors its `flow_events` log onto Laravel events you can listen to. Register listeners
the usual way (an event subscriber, `Event::listen`, or a listener class). Two events are the
exception and have no entry in that log — they report an outcome the engine **refused**, which never
became part of the run's history. See [A refused outcome](#refused-outcome).

```php
use DiscoveryUkraine\SagaLaraFlow\Events\FlowFailed;
use Illuminate\Support\Facades\Event;

Event::listen(FlowFailed::class, function (FlowFailed $event): void {
    report($event->flowRun->workflow_class.' failed: '.$event->flowRun->id);
});
```

:::tip The right place to report failures
`FlowFailed` is the recommended hook for **cross-cutting** failure handling (alerting, reporting,
metrics). It fires exactly once on the terminal transition — on both the direct-fail and the
fail-after-compensation paths, and regardless of whether the run was sync or queued — so you catch
every failed run in one place without wrapping `handle()`. Reserve `try/catch` inside `handle()` for
**local** branching within a single workflow (see [Actions › Handling failure](./actions.md)).
:::

## Available events

Flow lifecycle: `FlowStarted`, `FlowCompleted`, `FlowFailed`, `FlowWaiting`, `FlowResumed`,
`FlowRewoken`, `FlowCancelled`, `FlowExpired`.

Actions: `ActionStarted`, `ActionCompleted`, `ActionFailed`, `ActionRedispatched`,
`OptionalActionFailed`, `ActionAwaitingRetry`, `ActionRetried` (the last two cover
[retry on signal](./retry-on-signal.md)), `ActionOutcomeRejected`.

Compensations: `CompensationStarted`, `CompensationCompleted`, `CompensationFailed`,
`CompensationOutcomeRejected`.

Child workflows: `ChildWorkflowStarted`, `ChildWorkflowCompleted`, `ChildWorkflowFailed`,
`ChildWorkflowCancelled`.

Signals & side effects: `FlowSignalReceived`, `FlowSignalConsumed`, `SideEffectRecorded`,
`SideEffectReused`.

(See `src/Events` for the full list.)

## A refused outcome {#refused-outcome}

A step finishes, and by then its row has moved on — the run was cancelled, a reclaim handed the row
to a second worker, the monitor expired it. The engine refuses that outcome, which is the right call
for the run, and the work stays done with nowhere to record it. `ActionOutcomeRejected` and
`CompensationOutcomeRejected` carry what it produced:

```php
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionOutcomeRejected;

Event::listen(ActionOutcomeRejected::class, function (ActionOutcomeRejected $event): void {
    match ($event->outcome) {
        // The value the action returned. Reconcile it, or park it for a human.
        FlowEventType::ActionCompleted => Ledger::orphanedResult(
            $event->actionRun->id,
            $event->result,
        ),
        // The throw. It is not rethrown either — a job whose work was already
        // discarded must not fail — so this is the only place it is ever named.
        FlowEventType::ActionFailed => report($event->exception),
        default => null,
    };
});
```

`$event->result` is the value **in the form the row would have stored it** — the same shape
`$actionRun->result` carries on a step that was recorded, so the reconciliation you write works
either way, and a queued listener can be handed it. `$event->exception` is the throw itself.

The step's own row is `$event->actionRun` (`$event->compensationRun` on the compensation event), and it
reads as the row did **before** the refused write: the payload travels in the event and never on the
model, so nothing here is safe to save back. Only the payload is at stake: the run's state is
already settled by whoever won the row.

The refusal is journalled as `outcome_rejected` whether or not you listen (see
[Reclaim & recovery](./reclaim-and-recovery.md)). The log line names the loss but does not carry it:
`AnomalyLog` writes to the application's default channel, which nobody chose for business payloads
or gave a retention policy. That choice is the host's, which is what the event is for.

An expiry the same fence refuses is journalled under that reason too, and raises no event: the
exception it would have written is the monitor's own account of the deadline, not a value only the
worker held.

These two events are also the exception to the warning below. A listener that throws on either of
them — or a queued one the payload cannot be serialised with — is journalled as
`rejection_undelivered` and goes no further. The engine deliberately does not fail the job on this
path: a job that fails here runs its `failed()` hook, which would write queue bookkeeping into a row
the second worker now owns. The cost is that the discarded payload is then lost after all, so treat
that reason code as a defect in your listener rather than a race.

:::warning Listeners must be queued, or must not throw
These events are dispatched from inside the engine's replay. A synchronous listener that throws
interrupts the replay at that point, and the engine reads the exception as a business failure — it
can fail and compensate a run that was doing fine. Mark your listeners `ShouldQueue`, or make sure
they cannot throw. The two [rejection events](#refused-outcome) are the exception: a throw there is
journalled rather than propagated.
:::

## Cancellation reason

`FlowCancelled` carries an optional `?string $reason`, populated when you cancel through the handle:

```php
SagaFlow::loadFlow($runId)->cancel('superseded by a newer order');
```

The reason is recorded on the `flow.cancelled` event metadata (no schema change) and passed to the
`FlowCancelled` Laravel event:

```php
Event::listen(FlowCancelled::class, function (FlowCancelled $event): void {
    logger()->info('cancelled', ['id' => $event->flowRun->id, 'reason' => $event->reason]);
});
```
