<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Concerns\NormalizesExceptions;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowCancelled;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowCompleted;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowExpired;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowFailed;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowResumed;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowRewoken;
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

    /**
     * Record the doctor re-waking a flow: an automatic re-wake of a
     * stuck Waiting run (reason "lost_resume") or a manual saga-flow:kick (reason
     * "manual"). The run is only re-driven, never mutated here, so this is a marker
     * event that makes the intervention visible in history.
     */
    public function flowRewoken(FlowRun $flowRun, string $reason): void
    {
        $this->events->record($flowRun, FlowEventType::FlowRewoken, null, $flowRun, [
            'reason' => $reason,
        ]);

        event(new FlowRewoken($flowRun, $reason));
    }

    public function flowCancelled(FlowRun $flowRun, ?string $reason = null): void
    {
        $this->events->record(
            $flowRun,
            FlowEventType::FlowCancelled,
            null,
            $flowRun,
            $reason !== null ? ['reason' => $reason] : []
        );

        event(new FlowCancelled($flowRun, $reason));
    }

    /**
     * Record a monitor-enforced expiration: the run's exception payload (already set
     * by the time this is called) explains why it expired.
     */
    public function flowExpired(FlowRun $flowRun): void
    {
        $this->events->record(
            $flowRun,
            FlowEventType::FlowExpired,
            null,
            $flowRun,
            $flowRun->exception !== null ? ['exception' => $flowRun->exception] : []
        );

        event(new FlowExpired($flowRun));
    }
}
