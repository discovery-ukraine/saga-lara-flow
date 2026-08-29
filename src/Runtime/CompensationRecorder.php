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
use DiscoveryUkraine\SagaLaraFlow\Events\CompensationStepStarted;
use DiscoveryUkraine\SagaLaraFlow\Models\CompensationRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\DB;
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
        private AnomalyLog $anomalies,
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
            'reclaim_stale_after_seconds' => $this->resolveReclaimStaleAfterSeconds(
                $entry->reclaimStaleAfterSeconds,
                $entry->reclaimStaleEnabled,
            ),
        ]);

        $compensation->save();

        return $compensation;
    }

    /**
     * Same resolution shape as ActionRecorder::resolveReclaimStaleAfterSeconds(), read
     * against the sagas.reclaim.stale_running config instead — the two mechanisms are
     * switched independently.
     */
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
     * Atomically claim the row and move it to Running, returning false when the row is
     * no longer claimable — settled already, or still Running before its reclaim
     * deadline. Mirrors ActionRecorder::startAction() in every respect: the same
     * compare-and-swap shape, the same enclosing transaction, the same absence of
     * Eloquent model events (CompensationStepStarted and the flow_events entry are the
     * supported way to observe it), and the same attempts counter — including the
     * read-back that proves the claim survived its own commit.
     *
     * @throws Throwable
     */
    public function startCompensation(CompensationRun $compensation): bool
    {
        $claimed = $compensation->getConnection()->transaction(function () use ($compensation): bool {
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

    /**
     * The undo half of ActionRecorder::claimSurvivedCommit(), which carries the
     * reasoning: a commit reporting success is not proof the claim is on record, so the
     * row answers for itself — and what it answers is what this connection can see.
     */
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

    /**
     * Record the compensation's result, fenced against the claim that produced it —
     * see ActionRecorder::completeAction() for why. Returns whether it was recorded.
     */
    public function completeCompensation(CompensationRun $compensation, mixed $result): bool
    {
        $claimedAttempts = $compensation->attempts;

        $compensation->status = CompensationStatus::Completed;
        $compensation->result = $this->serializer->serialize($result);
        $compensation->finished_at = Carbon::now();

        if (! $this->writeOutcome($compensation, $claimedAttempts, FlowEventType::CompensationCompleted)) {
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

        if (! $this->writeOutcome($compensation, $claimedAttempts, FlowEventType::CompensationFailed)) {
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
     * Compensations have no expiry sweep, so the status condition guards nothing today;
     * it holds the invariant locally rather than depending on no such writer ever
     * being added.
     */
    private function writeOutcome(
        CompensationRun $compensation,
        int $claimedAttempts,
        FlowEventType $outcome
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

            return false;
        }

        $compensation->syncOriginal();

        return true;
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
