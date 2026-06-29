<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Concerns\NormalizesExceptions;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionCompleted;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionFailed;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionStarted;
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
    ) {}

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
        bool $hasCompensation = false
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
            'arguments' => $this->serializer->serialize($arguments),
            'attempts' => 0,
        ]);

        $actionRun->save();

        $this->events->record($flowRun, FlowEventType::ActionScheduled, $sequence, $actionRun, [
            'action_class' => $actionClass,
        ]);

        return $actionRun;
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
}
