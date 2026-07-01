---
id: events
title: Events
sidebar_position: 20
---

# Events

The engine mirrors its `flow_events` log onto Laravel events you can listen to. Register listeners
the usual way (an event subscriber, `Event::listen`, or a listener class).

```php
use DiscoveryUkraine\SagaLaraFlow\Events\FlowFailed;
use Illuminate\Support\Facades\Event;

Event::listen(FlowFailed::class, function (FlowFailed $event): void {
    report($event->flowRun->workflow_class.' failed: '.$event->flowRun->id);
});
```

## Available events

Flow lifecycle: `FlowStarted`, `FlowCompleted`, `FlowFailed`, `FlowWaiting`, `FlowResumed`,
`FlowRewoken`, `FlowCancelled`, `FlowExpired`.

Actions: `ActionStarted`, `ActionCompleted`, `ActionFailed`, `ActionRedispatched`,
`OptionalActionFailed`.

Compensations: `CompensationStarted`, `CompensationCompleted`, `CompensationFailed`.

Child workflows: `ChildWorkflowStarted`, `ChildWorkflowCompleted`, `ChildWorkflowFailed`,
`ChildWorkflowCancelled`.

Signals & side effects: `FlowSignalReceived`, `FlowSignalConsumed`, `SideEffectRecorded`,
`SideEffectReused`.

(See `src/Events` for the full list.)

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
