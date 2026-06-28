<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionCompleted;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionFailed;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionStarted;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowCompleted;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowFailed;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowResumed;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowStarted;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowWaiting;
use DiscoveryUkraine\SagaLaraFlow\Events\SideEffectRecorded;
use DiscoveryUkraine\SagaLaraFlow\Events\SideEffectReused;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\SideEffect;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Carbon;
use Throwable;

/**
 * Single point of truth for persisting workflow progress: it creates/updates
 * ActionRun rows, appends to the flow_events log, and dispatches the matching
 * Laravel event. Everything else in the runtime records steps through here.
 */
readonly class StepRecorder
{
    public function __construct(
        private Serializer $serializer,
    ) {}

    public function flowStarted(FlowRun $flowRun): void
    {
        $this->recordEvent($flowRun, FlowEventType::FlowStarted, null, $flowRun);

        event(new FlowStarted($flowRun));
    }

    public function flowResumed(FlowRun $flowRun): void
    {
        $this->recordEvent($flowRun, FlowEventType::FlowResumed, null, $flowRun);

        event(new FlowResumed($flowRun));
    }

    public function flowWaiting(FlowRun $flowRun): void
    {
        $this->recordEvent($flowRun, FlowEventType::FlowWaiting, null, $flowRun);

        event(new FlowWaiting($flowRun));
    }

    public function flowCompleted(FlowRun $flowRun): void
    {
        $this->recordEvent($flowRun, FlowEventType::FlowCompleted, null, $flowRun);

        event(new FlowCompleted($flowRun));
    }

    public function flowFailed(FlowRun $flowRun, Throwable $exception): void
    {
        $this->recordEvent($flowRun, FlowEventType::FlowFailed, null, $flowRun, [
            'exception' => $this->exceptionToArray($exception),
        ]);

        event(new FlowFailed($flowRun, $exception));
    }

    /**
     * Create the pending ActionRun for a scheduled step. The arguments are
     * serialized once here and become the durable source the executing job
     * (or inline run) reads back.
     *
     * @param  array<int, mixed>  $arguments
     */
    public function scheduleAction(FlowRun $flowRun, int $sequence, string $actionClass, array $arguments): ActionRun
    {
        /** @var class-string<ActionRun> $model */
        $model = config('saga-lara-flow.models.action_run');

        $actionRun = new $model;

        $actionRun->fill([
            'flow_run_id' => $flowRun->id,
            'sequence' => $sequence,
            'action_class' => $actionClass,
            'status' => ActionStatus::Pending,
            'arguments' => $this->serializer->serialize($arguments),
            'attempts' => 0,
        ]);

        $actionRun->save();

        $this->recordEvent($flowRun, FlowEventType::ActionScheduled, $sequence, $actionRun, [
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

        $this->recordEvent(
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

        $this->recordEvent(
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

        $this->recordEvent(
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
     * Persist a side effect's computed value the first time it is encountered.
     * The value is serialized once here and replayed verbatim on every later
     * pass; the key is a human label only, identity is the sequence.
     */
    public function recordSideEffect(FlowRun $flowRun, int $sequence, string $key, mixed $value): SideEffect
    {
        /** @var class-string<SideEffect> $model */
        $model = config('saga-lara-flow.models.side_effect');

        $sideEffect = new $model;

        $sideEffect->fill([
            'flow_run_id' => $flowRun->id,
            'sequence' => $sequence,
            'key' => $key,
            'value' => $this->serializer->serialize($value),
        ]);

        $sideEffect->save();

        $this->recordEvent($flowRun, FlowEventType::SideEffectRecorded, $sequence, $sideEffect, [
            'key' => $key,
        ]);

        event(new SideEffectRecorded($sideEffect));

        return $sideEffect;
    }

    /**
     * Signal that a side effect resolved from stored history on replay. This is
     * a silent replay resolution (like a completed action): by default it only
     * dispatches the Laravel event for observability and writes NO flow_events
     * row, so the log stays bounded across the many replays a single run
     * performs. Set history.record_side_effect_reuse to also persist a
     * side_effect.reused flow_events row on every reuse for a full audit trail.
     */
    public function sideEffectReused(FlowRun $flowRun, SideEffect $sideEffect): void
    {
        if (config('saga-lara-flow.history.record_side_effect_reuse', false)) {
            $this->recordEvent($flowRun, FlowEventType::SideEffectReused, $sideEffect->sequence, $sideEffect, [
                'key' => $sideEffect->key,
            ]);
        }

        event(new SideEffectReused($sideEffect));
    }

    /**
     * @param  array<string, mixed>  $payload
     */
    private function recordEvent(
        FlowRun $flowRun,
        FlowEventType $eventType,
        ?int $sequence,
        ?Model $subject,
        array $payload = []
    ): void {
        /** @var class-string<Model> $model */
        $model = config('saga-lara-flow.models.flow_event');

        $event = new $model;

        $event->fill([
            'flow_run_id' => $flowRun->id,
            'sequence' => $sequence,
            'type' => $eventType,
            'subject_type' => $subject !== null ? $subject::class : null,
            'subject_id' => $subject?->getKey(),
            'payload' => $payload === [] ? null : $payload,
            'recorded_at' => Carbon::now(),
        ]);

        $event->save();
    }

    /**
     * @return array<string, mixed>
     */
    private function exceptionToArray(Throwable $exception): array
    {
        return [
            'class' => $exception::class,
            'message' => $exception->getMessage(),
            'code' => $exception->getCode(),
        ];
    }
}
