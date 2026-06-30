<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DateTimeInterface;
use DiscoveryUkraine\SagaLaraFlow\Concerns\NormalizesExceptions;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionCompleted;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionFailed;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionStarted;
use DiscoveryUkraine\SagaLaraFlow\Events\OptionalActionFailed;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Support\Carbon;
use Throwable;

/**
 * Persists an action step through its lifecycle (scheduled → started → completed
 * /failed), serializing arguments and results and appending the matching events.
 */
final readonly class ActionRecorder
{
    use NormalizesExceptions;

    public function __construct(
        private EventLog $events,
        private Serializer $serializer,
    ) {
    }

    /**
     * Create the pending ActionRun for a scheduled step. The arguments are
     * serialized once here and become the durable source the executing job
     * (or inline run) reads back.
     *
     * @param  array<int, mixed>  $arguments
     */
    public function scheduleAction(
        FlowRun $flowRun,
        int $sequence,
        string $actionClass,
        array $arguments,
        bool $hasCompensation = false,
        bool $continueOnFailure = false,
        ?int $parallelGroup = null,
        ?DateTimeInterface $expiresAt = null,
    ): ActionRun {
        /** @var class-string<ActionRun> $model */
        $model = config('saga-lara-flow.models.action_run');

        $actionRun = new $model;

        $actionRun->fill([
            'flow_run_id' => $flowRun->id,
            'sequence' => $sequence,
            'action_class' => $actionClass,
            'status' => ActionStatus::Pending,
            'has_compensation' => $hasCompensation,
            'continue_on_failure' => $continueOnFailure,
            'parallel_group' => $parallelGroup,
            'expires_at' => $expiresAt ?? $this->defaultExpiry(),
            'arguments' => $this->serializer->serialize($arguments),
            'attempts' => 0,
        ]);

        $actionRun->save();

        $this->events->record($flowRun, FlowEventType::ActionScheduled, $sequence, $actionRun, [
            'action_class' => $actionClass,
        ]);

        return $actionRun;
    }

    private function defaultExpiry(): ?DateTimeInterface
    {
        $seconds = config('saga-lara-flow.monitor.expiration.defaults.action');

        return $seconds === null ? null : Carbon::now()->addSeconds((int) $seconds);
    }

    public function startAction(ActionRun $actionRun): void
    {
        $actionRun->status = ActionStatus::Running;
        $actionRun->attempts = $actionRun->attempts + 1;
        $actionRun->started_at = Carbon::now();
        $actionRun->save();

        $this->events->record(
            $actionRun->flowRun,
            FlowEventType::ActionStarted,
            $actionRun->sequence,
            $actionRun
        );

        event(new ActionStarted($actionRun));
    }

    public function completeAction(ActionRun $actionRun, mixed $result): void
    {
        $actionRun->status = ActionStatus::Completed;
        $actionRun->result = $this->serializer->serialize($result);
        $actionRun->finished_at = Carbon::now();
        $actionRun->save();

        $this->events->record(
            $actionRun->flowRun,
            FlowEventType::ActionCompleted,
            $actionRun->sequence,
            $actionRun
        );

        event(new ActionCompleted($actionRun));
    }

    public function failAction(ActionRun $actionRun, Throwable $exception): void
    {
        $exceptionArray = $this->exceptionToArray($exception);

        $actionRun->status = ActionStatus::Failed;
        $actionRun->exception = $exceptionArray;
        $actionRun->finished_at = Carbon::now();
        $actionRun->save();

        $this->events->record(
            $actionRun->flowRun,
            FlowEventType::ActionFailed,
            $actionRun->sequence,
            $actionRun,
            [
                'exception' => $exceptionArray,
            ]
        );

        event(new ActionFailed($actionRun, $exception));
    }

    /**
     * Mark a failed optional (continueOnFailure) step as OptionalFailed once its
     * retries are exhausted. The flow is not failed; the recorded exception is
     * preserved and an optional_failed event/Laravel event is appended so the
     * give-up is visible in history.
     */
    public function optionalFail(ActionRun $actionRun): void
    {
        $actionRun->status = ActionStatus::OptionalFailed;
        $actionRun->finished_at = Carbon::now();
        $actionRun->save();

        $this->events->record(
            $actionRun->flowRun,
            FlowEventType::ActionOptionalFailed,
            $actionRun->sequence,
            $actionRun,
            $actionRun->exception !== null ? ['exception' => $actionRun->exception] : []
        );

        event(new OptionalActionFailed($actionRun));
    }

    /**
     * Mark a still-pending/running step Expired once its expires_at deadline passes
     * (monitor, §15): record the expiry cause and append an action.expired event. On
     * replay the seam treats Expired as a failure (or, for an optional step, as a
     * give-up returning its fallback).
     *
     * @param  array<string, mixed>  $exception
     */
    public function expireAction(ActionRun $actionRun, array $exception): void
    {
        $actionRun->status = ActionStatus::Expired;
        $actionRun->exception = $exception;
        $actionRun->finished_at = Carbon::now();
        $actionRun->save();

        $this->events->record(
            $actionRun->flowRun,
            FlowEventType::ActionExpired,
            $actionRun->sequence,
            $actionRun,
            ['exception' => $exception],
        );
    }
}
