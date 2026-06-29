<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Concerns\NormalizesExceptions;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationFailurePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Events\CompensationCompleted;
use DiscoveryUkraine\SagaLaraFlow\Events\CompensationFailed;
use DiscoveryUkraine\SagaLaraFlow\Events\CompensationStarted;
use DiscoveryUkraine\SagaLaraFlow\Models\CompensationRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Support\Carbon;
use Throwable;

/**
 * Persists the compensation lifecycle (pending → running → completed/failed) and
 * dispatches the matching events.
 */
final readonly class CompensationRecorder
{
    use NormalizesExceptions;

    public function __construct(
        private EventLog $events,
        private Serializer $serializer,
    ) {}

    /**
     * Announce that the run has entered rollback. The per-compensation rows follow
     * via register(); this is the once-per-run "compensation.started" marker.
     */
    public function started(FlowRun $flowRun): void
    {
        $this->events->record($flowRun, FlowEventType::CompensationStarted, null, $flowRun);

        event(new CompensationStarted($flowRun));
    }

    /**
     * Create the pending CompensationRun for one stack entry. Its rollback ordinal
     * (sequence) is the number of compensations already registered for the run, so
     * the rows read back in execution order. continue_on_failure encodes the
     * resolved policy (Continue = true), which the queued continuation reads to
     * decide whether a failed level halts the rollback.
     */
    public function register(
        FlowRun $flowRun,
        CompensationEntry $entry,
        CompensationFailurePolicy $policy
    ): CompensationRun {
        $definition = $entry->definition;

        $compensation = $this->newCompensation();

        $compensation->fill([
            'flow_run_id' => $flowRun->id,
            'action_run_id' => $entry->actionRunId,
            'sequence' => $this->nextSequence($flowRun),
            'compensation_type' => $definition->type,
            'compensation_class' => $definition->class,
            'status' => CompensationStatus::Pending,
            'continue_on_failure' => $policy === CompensationFailurePolicy::Continue,
            'arguments' => $definition->isClosure() ? null : $definition->arguments,
        ]);

        $compensation->save();

        return $compensation;
    }

    public function startCompensation(CompensationRun $compensation): void
    {
        $compensation->status = CompensationStatus::Running;
        $compensation->started_at = Carbon::now();
        $compensation->save();
    }

    public function completeCompensation(CompensationRun $compensation, mixed $result): void
    {
        $compensation->status = CompensationStatus::Completed;
        $compensation->result = $this->serializer->serialize($result);
        $compensation->finished_at = Carbon::now();
        $compensation->save();

        $this->events->record(
            $compensation->flowRun,
            FlowEventType::CompensationCompleted,
            $compensation->sequence,
            $compensation,
        );

        event(new CompensationCompleted($compensation));
    }

    public function failCompensation(CompensationRun $compensation, Throwable $exception): void
    {
        $exceptionArray = $this->exceptionToArray($exception);

        $compensation->status = CompensationStatus::Failed;
        $compensation->exception = $exceptionArray;
        $compensation->finished_at = Carbon::now();
        $compensation->save();

        $this->events->record(
            $compensation->flowRun,
            FlowEventType::CompensationFailed,
            $compensation->sequence,
            $compensation,
            ['exception' => $exceptionArray],
        );

        event(new CompensationFailed($compensation, $exception));
    }

    private function nextSequence(FlowRun $flowRun): int
    {
        return $this->newCompensation()->newQuery()
            ->where('flow_run_id', $flowRun->id)
            ->count();
    }

    private function newCompensation(): CompensationRun
    {
        /** @var class-string<CompensationRun> $model */
        $model = config('saga-lara-flow.models.compensation_run');

        return new $model;
    }
}
