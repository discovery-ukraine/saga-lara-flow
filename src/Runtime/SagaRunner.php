<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Concerns\NormalizesExceptions;
use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Enums\StepExecution;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionClaimFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CompensationFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CompensationUnfinishedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ConcurrentFlowTransitionException;
use DiscoveryUkraine\SagaLaraFlow\Jobs\RunCompensationJob;
use DiscoveryUkraine\SagaLaraFlow\Models\CompensationRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Support\Facades\Bus;
use Illuminate\Support\Str;
use Throwable;

/**
 * Executes the compensation stack in reverse (LIFO). Action-level compensations
 * are singleton levels run sequentially; a saga() group asked to compensate in
 * parallel forms one level rolled back together (Bus::batch in queued mode, still
 * sequential in sync). Levels are always processed in reverse order.
 *
 * The Stop/Continue policy is read from each compensation row's continue_on_failure
 * flag after a level finishes: Stop halts the rollback, Continue carries on; either
 * way the run lands in its final state (Failed for an automatic failure, Cancelled
 * for a manual compensate) with the primary and compensation causes recorded.
 */
class SagaRunner
{
    use NormalizesExceptions;

    public function __construct(
        private readonly CompensationRecorder $recorder,
        private readonly CompensationExecutor $executor,
        private readonly FlowLifecycleRecorder $lifecycle,
        private readonly FlowRepository $repository,
    ) {}

    /**
     * @param  list<CompensationEntry>  $entries
     * @param  array<string, mixed>|null  $primary
     *
     * @throws Throwable
     */
    public function rollback(
        FlowRun $flowRun,
        array $entries,
        ?array $primary,
        RunMode $mode,
        FlowStatus $finalState
    ): void {
        $levels = $this->levels($entries);

        if ($mode === RunMode::Sync) {
            $this->rollbackSync($flowRun, $levels, $primary, $finalState);

            return;
        }

        $this->dispatchLevel($flowRun, $levels, $primary, $finalState, $flowRun->rollback_token);
    }

    /**
     * Batch-completion continuation (queued mode): inspect the level that just ran,
     * halt on a Stop-policy failure, otherwise dispatch the next level or finalize.
     *
     * @param  list<list<CompensationEntry>>  $remainingLevels
     * @param  array<string, mixed>|null  $primary
     * @param  list<string>  $ranCompensationIds
     * @param  ?string  $rollbackToken  the owner the rollback was dispatched under; null for a
     *                                  continuation queued before the run carried one
     *
     * @throws Throwable
     */
    public function advance(
        string $flowRunId,
        array $remainingLevels,
        ?array $primary,
        FlowStatus $finalState,
        array $ranCompensationIds,
        ?string $rollbackToken = null,
    ): void {
        $flowRun = $this->repository->findOnWriter($flowRunId);

        // A rollback taken over since this level was sent is driven by its new owner.
        if ($flowRun === null || ($rollbackToken !== null && $flowRun->rollback_token !== $rollbackToken)) {
            return;
        }

        try {
            if ($this->levelStopped($ranCompensationIds)) {
                $this->finalize($flowRun, $finalState, $primary);

                return;
            }

            $this->dispatchLevel($flowRun, $remainingLevels, $primary, $finalState, $rollbackToken);
        } catch (ConcurrentFlowTransitionException) {
            // Another actor finished the run while the compensations were in flight; what
            // they ran is recorded either way. A batch callback is the one landing with no
            // caller to tell, so it absorbs here rather than in finalize().
        }
    }

    /**
     * @param  list<list<CompensationEntry>>  $levels
     * @param  array<string, mixed>|null  $primary
     *
     * @throws Throwable
     */
    private function rollbackSync(FlowRun $flowRun, array $levels, ?array $primary, FlowStatus $finalState): void
    {
        foreach ($levels as $level) {
            if (! $this->stillOwns($flowRun)) {
                return;
            }

            $stopped = $this->runLevelSync($flowRun, $level);

            if ($stopped === null) {
                return;
            }

            if ($stopped) {
                break;
            }
        }

        if ($this->stillOwns($flowRun)) {
            $this->finalize($flowRun, $finalState, $primary);
        }
    }

    /**
     * Whether the row still names this instance as the owner of its rollback. A rollback
     * that stalled long enough to be taken over is left to whoever took it.
     */
    private function stillOwns(FlowRun $flowRun): bool
    {
        if ($flowRun->rollback_token === null) {
            return true;
        }

        return $flowRun->newQuery()->useWritePdo()->whereKey($flowRun->getKey())
            ->value('rollback_token') === $flowRun->rollback_token;
    }

    /**
     * @param  list<CompensationEntry>  $level
     * @return ?bool whether a Stop-policy compensation failed (rollback must halt), or null
     *               once the rollback was taken over by another owner
     *
     * @throws Throwable
     */
    private function runLevelSync(FlowRun $flowRun, array $level): ?bool
    {
        $compensationIds = [];

        foreach ($level as $entry) {
            $compensation = $this->recorder->register($flowRun, $entry, $entry->effectivePolicy());

            $compensationIds[] = $compensation->id;

            if ($this->settled($compensation)
                || $this->executor->execute($compensation, $entry->definition, $flowRun->rollback_token) !== StepExecution::ClaimLost) {
                continue;
            }

            // A row an earlier owner settled is not run again. Losing the claim on any
            // other leaves the rest to the owner that took the rollback over, or stops it
            // loudly with the run still in Cancelling: a worker that may still be running
            // the undo holds the row, or the claim did not survive its own commit.
            // Recovery carries the rollback on once the row can be claimed, instead of it
            // landing over an undo that never ran.
            if (! $this->stillOwns($flowRun)) {
                return null;
            }

            throw ActionClaimFailedException::forCompensation(
                $entry->definition->class ?? $entry->definition->type,
                $compensation->sequence,
            );
        }

        return $this->levelStopped($compensationIds);
    }

    /**
     * Send the next level that still has something to run. A level an earlier owner
     * already settled is judged as it stands and passed over without a batch. A level
     * with a row still held by a worker that may be alive is neither sent nor judged:
     * the rollback stays where it is until that row settles, and is taken over again.
     *
     * @param  list<list<CompensationEntry>>  $levels
     * @param  array<string, mixed>|null  $primary
     *
     * @throws Throwable
     */
    private function dispatchLevel(
        FlowRun $flowRun,
        array $levels,
        ?array $primary,
        FlowStatus $finalState,
        ?string $rollbackToken,
    ): void {
        while ($levels !== []) {
            $head = array_shift($levels);

            [$compensationIds, $jobs, $held] = $this->registerLevel($flowRun, $head, $rollbackToken);

            if ($held) {
                return;
            }

            if ($jobs !== []) {
                $this->sendLevel($flowRun, $jobs, new AdvanceCompensation(
                    $flowRun->id,
                    $levels,
                    $primary,
                    $finalState,
                    $compensationIds,
                    $rollbackToken,
                ));

                return;
            }

            if ($this->levelStopped($compensationIds)) {
                break;
            }
        }

        $this->finalize($flowRun, $finalState, $primary);
    }

    /**
     * Register one level and build a job for each row still to run.
     *
     * @param  list<CompensationEntry>  $level
     * @return array{0: list<string>, 1: list<RunCompensationJob>, 2: bool} the level's row ids,
     *                                                                      its jobs, and whether a
     *                                                                      live worker holds a row
     */
    private function registerLevel(FlowRun $flowRun, array $level, ?string $rollbackToken): array
    {
        $compensationIds = [];
        $jobs = [];
        $held = false;

        foreach ($level as $entry) {
            $compensation = $this->recorder->register($flowRun, $entry, $entry->effectivePolicy());

            $compensationIds[] = $compensation->id;
            $held = $held || $this->held($compensation);

            if (! $this->settled($compensation)) {
                $jobs[] = new RunCompensationJob($flowRun->id, $compensation->id, $entry->definition, $rollbackToken);
            }
        }

        return [$compensationIds, $jobs, $held];
    }

    /**
     * @param  list<RunCompensationJob>  $jobs
     *
     * @throws Throwable
     */
    private function sendLevel(FlowRun $flowRun, array $jobs, AdvanceCompensation $advance): void
    {
        $batch = Bus::batch($jobs)
            ->name("saga-compensate:{$flowRun->id}")
            ->allowFailures()
            ->finally($advance);

        if ($flowRun->connection !== null) {
            $batch->onConnection($flowRun->connection);
        }

        if ($flowRun->queue !== null) {
            $batch->onQueue($flowRun->queue);
        }

        $batch->dispatch();
    }

    private function settled(CompensationRun $compensation): bool
    {
        return in_array($compensation->status, [CompensationStatus::Completed, CompensationStatus::Failed], true);
    }

    /**
     * Running and not yet claimable again: the worker that took it may still be running it.
     */
    private function held(CompensationRun $compensation): bool
    {
        return $compensation->status === CompensationStatus::Running
            && ($compensation->reclaim_stale_at === null || $compensation->reclaim_stale_at->isFuture());
    }

    /**
     * Land the run once the rollback is done.
     *
     * A refused landing is raised, not swallowed: only the caller knows whether losing
     * the run matters. FlowHandle::compensate() is an operator who has to be told.
     *
     * @param  array<string, mixed>|null  $primary
     */
    private function finalize(FlowRun $flowRun, FlowStatus $finalState, ?array $primary): void
    {
        if ($finalState === FlowStatus::Cancelled) {
            $flowRun->markCancelled();

            $this->lifecycle->flowCancelled($flowRun);

            app(ChildWorkflowManager::class)->onFlowFinalized($flowRun, true);

            return;
        }

        $exception = $primary ?? [];

        $compensation = $this->compensationOutcome($flowRun);

        if ($compensation !== null) {
            $exception['compensation'] = $compensation;
        }

        // A monitor-enforced expiration that rolled back lands in Expired, not Failed:
        // same exception bookkeeping, but the run's own terminal status and event differ.
        if ($finalState === FlowStatus::Expired) {
            $flowRun->exception = $exception === [] ? null : $exception;

            $flowRun->markExpired();

            $this->lifecycle->flowExpired($flowRun);

            app(ChildWorkflowManager::class)->onFlowFinalized($flowRun, true);

            return;
        }

        $flowRun->markFailed($exception === [] ? null : $exception);

        $this->lifecycle->flowFailedFromArray($flowRun, $primary);

        app(ChildWorkflowManager::class)->onFlowFinalized($flowRun, true);
    }

    /**
     * Cancel a run whose rollback has not finished. The owner token is replaced first, so the
     * owner claims nothing more, and only then is what the rollback left undone read and
     * recorded where a finished rollback records its failure.
     */
    public function abandon(FlowRun $flowRun): void
    {
        $token = (string) Str::ulid();

        $flowRun->newQuery()->whereKey($flowRun->getKey())
            ->where('status', FlowStatus::Cancelling)
            ->update(['rollback_token' => $token]);

        $flowRun->rollback_token = $token;

        if ($flowRun->rollback_compensates) {
            $flowRun->exception = ($flowRun->rollback_cause ?? []) + [
                'compensation' => $this->compensationOutcome($flowRun)
                    ?? $this->exceptionToArray(CompensationUnfinishedException::cutShort($flowRun)),
            ];
        }

        $flowRun->markCancelled();

        if ($flowRun->rollback_compensates) {
            app(AnomalyLog::class)->log(AnomalyLog::REASON_ROLLBACK_ABANDONED, [
                'entity' => 'flow',
                'flow_run_id' => $flowRun->id,
                'workflow_class' => $flowRun->workflow_class,
                'rollback_target' => $flowRun->rollback_target?->value,
            ]);
        }
    }

    /**
     * The first compensation that failed, or else the first that never settled: a run that
     * did not fully unwind never looks like one that did, and a real failure carries its own
     * cause. One read, so a row settling meanwhile is seen in one state or the other.
     *
     * @return array<string, mixed>|null
     */
    private function compensationOutcome(FlowRun $flowRun): ?array
    {
        $row = $this->compensationModel()->newQuery()
            ->useWritePdo()
            ->where('flow_run_id', $flowRun->id)
            ->whereIn('status', [CompensationStatus::Failed, CompensationStatus::Pending, CompensationStatus::Running])
            ->orderByRaw('CASE WHEN status = ? THEN 0 ELSE 1 END', [CompensationStatus::Failed->value])
            ->orderBy('sequence')
            ->first();

        if ($row === null) {
            return null;
        }

        return $row->status === CompensationStatus::Failed
            ? $this->exceptionToArray(CompensationFailedException::for($row)) + ['cause' => $row->exception]
            : $this->exceptionToArray(CompensationUnfinishedException::for($row));
    }

    /**
     * Split the stack into reverse-ordered levels: consecutive entries sharing a
     * parallelGroupId become one (parallel) level; everything else is a singleton.
     *
     * @param  list<CompensationEntry>  $entries
     * @return list<list<CompensationEntry>>
     */
    private function levels(array $entries): array
    {
        $reversed = array_reverse($entries);

        $levels = [];
        $index = 0;
        $count = count($reversed);

        while ($index < $count) {
            $groupId = $reversed[$index]->parallelGroupId;

            if ($groupId === null) {
                $levels[] = [$reversed[$index]];

                $index++;

                continue;
            }

            $level = [];

            while ($index < $count && $reversed[$index]->parallelGroupId === $groupId) {
                $level[] = $reversed[$index];

                $index++;
            }

            $levels[] = $level;
        }

        return $levels;
    }

    /**
     * Whether a Stop-policy compensation on the level did not complete.
     *
     * Wider than "failed": a compensation still Pending or Running when its level
     * finishes is one whose job was lost or whose worker died, and unwinding further
     * on top of a step that may never have been undone is what Stop exists to prevent.
     *
     * @param  list<string>  $compensationIds
     */
    private function levelStopped(array $compensationIds): bool
    {
        if ($compensationIds === []) {
            return false;
        }

        return $this->compensationModel()->newQuery()
            ->useWritePdo()
            ->whereIn('id', $compensationIds)
            ->whereNot('status', CompensationStatus::Completed)
            ->where('continue_on_failure', false)
            ->exists();
    }

    private function compensationModel(): CompensationRun
    {
        /** @var class-string<CompensationRun> $model */
        $model = config('saga-lara-flow.models.compensation_run');

        return new $model;
    }
}
