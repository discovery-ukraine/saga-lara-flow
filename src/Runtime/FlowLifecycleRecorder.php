<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Concerns\NormalizesExceptions;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowCompleted;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowFailed;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowResumed;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowStarted;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowWaiting;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Throwable;

/**
 * Records the run's lifecycle transitions to the event log and dispatches the
 * matching Laravel events.
 */
final readonly class FlowLifecycleRecorder
{
    use NormalizesExceptions;

    public function __construct(
        private EventLog $events,
    ) {}

    public function flowStarted(FlowRun $flowRun): void
    {
        $this->events->record($flowRun, FlowEventType::FlowStarted, null, $flowRun);

        event(new FlowStarted($flowRun));
    }

    public function flowResumed(FlowRun $flowRun): void
    {
        $this->events->record($flowRun, FlowEventType::FlowResumed, null, $flowRun);

        event(new FlowResumed($flowRun));
    }

    public function flowWaiting(FlowRun $flowRun): void
    {
        $this->events->record($flowRun, FlowEventType::FlowWaiting, null, $flowRun);

        event(new FlowWaiting($flowRun));
    }

    public function flowCompleted(FlowRun $flowRun): void
    {
        $this->events->record($flowRun, FlowEventType::FlowCompleted, null, $flowRun);

        event(new FlowCompleted($flowRun));
    }

    public function flowFailed(FlowRun $flowRun, Throwable $exception): void
    {
        $this->events->record($flowRun, FlowEventType::FlowFailed, null, $flowRun, [
            'exception' => $this->exceptionToArray($exception),
        ]);

        event(new FlowFailed($flowRun, $exception));
    }
}
