<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Concerns\NormalizesExceptions;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowCancelled;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowCompleted;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowFailed;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowResumed;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowStarted;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowWaiting;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowException;
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

    /**
     * Record a flow failure from a normalized exception array (used by the
     * compensation finalize path, where the original throwable is not carried —
     * its trace may hold unserializable closures across the queued continuation).
     *
     * @param  array<string, mixed>|null  $exception
     */
    public function flowFailedFromArray(FlowRun $flowRun, ?array $exception): void
    {
        $this->events->record(
            $flowRun,
            FlowEventType::FlowFailed,
            null,
            $flowRun,
            $exception !== null ? [
                'exception' => $exception,
            ] : []
        );

        $message = is_string($exception['message'] ?? null) ? $exception['message'] : 'flow failed';

        event(new FlowFailed($flowRun, new FlowException($message)));
    }

    public function flowCancelled(FlowRun $flowRun): void
    {
        $this->events->record($flowRun, FlowEventType::FlowCancelled, null, $flowRun);

        event(new FlowCancelled($flowRun));
    }
}
