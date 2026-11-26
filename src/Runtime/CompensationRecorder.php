<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Concerns\NormalizesExceptions;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationFailurePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Events\CompensationCompleted;
use DiscoveryUkraine\SagaLaraFlow\Events\CompensationFailed;
use DiscoveryUkraine\SagaLaraFlow\Events\CompensationOutcomeRejected;
use DiscoveryUkraine\SagaLaraFlow\Events\CompensationStarted;
use DiscoveryUkraine\SagaLaraFlow\Events\CompensationStepStarted;
use DiscoveryUkraine\SagaLaraFlow\Models\CompensationRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\DB;
use Throwable;

final readonly class CompensationRecorder
{
    use NormalizesExceptions;

    public function __construct(
        private EventLog $events,
        private Serializer $serializer,
        private AnomalyLog $anomalies,
    ) {}

    public function started(FlowRun $flowRun): void
    {
        $this->events->record($flowRun, FlowEventType::CompensationStarted, null, $flowRun);

        event(new CompensationStarted($flowRun));
    }

    /**
     * Reuses the row already registered for the step, so a resumed or retried rollback runs no
     * compensation twice.
     */
    public function register(
        FlowRun $flowRun,
        CompensationEntry $entry,
        CompensationFailurePolicy $policy
    ): CompensationRun {
        $definition = $entry->definition;

        return $this->newCompensation()->newQuery()->useWritePdo()->firstOrCreate(
            ['flow_run_id' => $flowRun->id, 'step_sequence' => $entry->sequence],
            fn (): array => [
                'action_run_id' => $entry->actionRunId,
                'sequence' => $this->nextSequence($flowRun),
                'compensation_type' => $definition->type,
                'compensation_class' => $definition->class,
                'status' => CompensationStatus::Pending,
                'continue_on_failure' => $policy === CompensationFailurePolicy::Continue,
                'arguments' => $definition->isClosure() ? null : $definition->arguments,
                'reclaim_stale_after_seconds' => $this->resolveReclaimStaleAfterSeconds(
                    $entry->reclaimStaleAfterSeconds,
                    $entry->reclaimStaleEnabled,
                ),
            ],
        );
    }

    private function resolveReclaimStaleAfterSeconds(?int $seconds, ?bool $enabled): ?int
    {
        if ($enabled === false) {
            return null;
        }

        if ($seconds !== null) {
            return $seconds;
        }

        if ($enabled === true) {
            return (int) config('saga-lara-flow.sagas.reclaim.stale_running.after_seconds');
        }

        return config('saga-lara-flow.sagas.reclaim.stale_running.enabled')
            ? (int) config('saga-lara-flow.sagas.reclaim.stale_running.after_seconds')
            : null;
    }

    /**
     * Refused once the rollback has another owner than $rollbackToken; null, from a job queued
     * before runs carried one, claims for whoever owns it.
     *
     * @throws Throwable
     */
    public function startCompensation(CompensationRun $compensation, ?string $rollbackToken = null): bool
    {
        $claimed = $compensation->getConnection()->transaction(function () use ($compensation, $rollbackToken): bool {
            $now = Carbon::now();

            $staleAfter = $compensation->reclaim_stale_after_seconds;

            $claimed = $compensation->newQuery()
                ->whereKey($compensation->getKey())
                ->where(function ($query) use ($now): void {
                    $query->where('status', CompensationStatus::Pending)
                        ->orWhere(function ($query) use ($now): void {
                            $query->where('status', CompensationStatus::Running)
                                ->whereNotNull('reclaim_stale_at')
                                ->where('reclaim_stale_at', '<=', $now);
                        });
                })
                ->when($rollbackToken !== null, fn ($query) => $query->whereHas(
                    'flowRun',
                    fn ($run) => $run->where('rollback_token', $rollbackToken),
                ))
                ->update([
                    'status' => CompensationStatus::Running,
                    'attempts' => DB::raw('attempts + 1'),
                    'started_at' => $now,
                    'reclaim_stale_at' => $staleAfter === null
                        ? null
                        : $now->copy()->addSeconds($staleAfter),
                ]) === 1;

            if (! $claimed) {
                $this->anomalies->log(AnomalyLog::REASON_CLAIM_LOST, [
                    'entity' => 'compensation',
                    'flow_run_id' => $compensation->flow_run_id,
                    'compensation_run_id' => $compensation->id,
                    'sequence' => $compensation->sequence,
                    'compensation_class' => $compensation->compensation_class,
                ]);

                return false;
            }

            $compensation->refresh();

            $this->events->record(
                $compensation->flowRun,
                FlowEventType::CompensationStepStarted,
                $compensation->sequence,
                $compensation,
            );

            event(new CompensationStepStarted($compensation));

            return true;
        });

        return $claimed && $this->claimSurvivedCommit($compensation);
    }

    private function claimSurvivedCommit(CompensationRun $compensation): bool
    {
        $claimedAttempts = $compensation->attempts;

        $stored = $compensation->newQuery()
            ->useWritePdo()
            ->whereKey($compensation->getKey())
            ->first(['status', 'attempts']);

        if ($stored?->status === CompensationStatus::Running && $stored->attempts === $claimedAttempts) {
            return true;
        }

        $this->anomalies->log(
            $stored === null || $stored->attempts < $claimedAttempts
                ? AnomalyLog::REASON_CLAIM_NOT_COMMITTED
                : AnomalyLog::REASON_CLAIM_LOST,
            [
                'entity' => 'compensation',
                'flow_run_id' => $compensation->flow_run_id,
                'compensation_run_id' => $compensation->id,
                'sequence' => $compensation->sequence,
                'compensation_class' => $compensation->compensation_class,
                'claimed_attempts' => $claimedAttempts,
                'stored_attempts' => $stored?->attempts,
                'stored_status' => $stored?->status->value,
            ]
        );

        return false;
    }

    public function completeCompensation(CompensationRun $compensation, mixed $result): bool
    {
        $claimedAttempts = $compensation->attempts;

        $compensation->status = CompensationStatus::Completed;
        $compensation->result = $this->serializer->serialize($result);
        $compensation->finished_at = Carbon::now();

        $written = $this->writeOutcome(
            $compensation,
            $claimedAttempts,
            FlowEventType::CompensationCompleted,
            result: $compensation->result,
        );

        if (! $written) {
            return false;
        }

        $this->events->record(
            $compensation->flowRun,
            FlowEventType::CompensationCompleted,
            $compensation->sequence,
            $compensation,
        );

        event(new CompensationCompleted($compensation));

        return true;
    }

    public function failCompensation(CompensationRun $compensation, Throwable $exception): bool
    {
        $claimedAttempts = $compensation->attempts;
        $exceptionArray = $this->exceptionToArray($exception);

        $compensation->status = CompensationStatus::Failed;
        $compensation->exception = $exceptionArray;
        $compensation->finished_at = Carbon::now();

        $written = $this->writeOutcome(
            $compensation,
            $claimedAttempts,
            FlowEventType::CompensationFailed,
            exception: $exception,
        );

        if (! $written) {
            return false;
        }

        $this->events->record(
            $compensation->flowRun,
            FlowEventType::CompensationFailed,
            $compensation->sequence,
            $compensation,
            ['exception' => $exceptionArray],
        );

        event(new CompensationFailed($compensation, $exception));

        return true;
    }

    /**
     * @see ActionRecorder::writeOutcome()
     *
     * No writer expires a compensation today; the status condition keeps the fence from depending
     * on that staying true.
     */
    private function writeOutcome(
        CompensationRun $compensation,
        int $claimedAttempts,
        FlowEventType $outcome,
        mixed $result = null,
        ?Throwable $exception = null,
    ): bool {
        $written = $compensation->newQuery()
            ->whereKey($compensation->getKey())
            ->where('attempts', $claimedAttempts)
            ->where('status', CompensationStatus::Running)
            ->update($compensation->getDirty()) === 1;

        if (! $written) {
            $this->anomalies->log(AnomalyLog::REASON_OUTCOME_REJECTED, [
                'entity' => 'compensation',
                'flow_run_id' => $compensation->flow_run_id,
                'compensation_run_id' => $compensation->id,
                'sequence' => $compensation->sequence,
                'compensation_class' => $compensation->compensation_class,
                'outcome' => $outcome->value,
                'claimed_attempts' => $claimedAttempts,
            ]);

            $compensation->discardChanges();

            $this->announceRejection(
                new CompensationOutcomeRejected($compensation, $outcome, $result, $exception),
                $compensation,
            );

            return false;
        }

        $compensation->syncOriginal();

        return true;
    }

    /**
     * @see ActionRecorder::announceRejection()
     */
    private function announceRejection(
        CompensationOutcomeRejected $rejection,
        CompensationRun $compensation
    ): void {
        try {
            event($rejection);
        } catch (Throwable $undelivered) {
            $this->anomalies->log(AnomalyLog::REASON_REJECTION_UNDELIVERED, [
                'entity' => 'compensation',
                'flow_run_id' => $compensation->flow_run_id,
                'compensation_run_id' => $compensation->id,
                'sequence' => $compensation->sequence,
                'compensation_class' => $compensation->compensation_class,
                'outcome' => $rejection->outcome->value,
                'exception' => $undelivered::class,
                'message' => $undelivered->getMessage(),
            ]);
        }
    }

    private function nextSequence(FlowRun $flowRun): int
    {
        return $this->newCompensation()->newQuery()
            ->useWritePdo()
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
