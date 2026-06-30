<?php

namespace DiscoveryUkraine\SagaLaraFlow\Enums;

enum FlowEventType: string
{
    case FlowCreated = 'flow.created';
    case FlowStarted = 'flow.started';
    case FlowWaiting = 'flow.waiting';
    case FlowResumed = 'flow.resumed';
    case FlowCompleted = 'flow.completed';
    case FlowFailed = 'flow.failed';
    case FlowCancelled = 'flow.cancelled';
    case FlowExpired = 'flow.expired';

    case ActionScheduled = 'action.scheduled';
    case ActionStarted = 'action.started';
    case ActionCompleted = 'action.completed';
    case ActionFailed = 'action.failed';
    case ActionOptionalFailed = 'action.optional_failed';
    case ActionExpired = 'action.expired';

    case SignalReceived = 'signal.received';
    case SignalConsumed = 'signal.consumed';
    case SignalTimedOut = 'signal.timed_out';

    case SideEffectRecorded = 'side_effect.recorded';
    case SideEffectReused = 'side_effect.reused';

    case CompensationStarted = 'compensation.started';
    case CompensationCompleted = 'compensation.completed';
    case CompensationFailed = 'compensation.failed';

    case ChildStarted = 'child.started';
    case ChildCompleted = 'child.completed';
    case ChildFailed = 'child.failed';
    case ChildCancelled = 'child.cancelled';
}
