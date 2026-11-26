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
            // A batch callback has no caller to tell, so a landing lost to another actor ends here.
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

            // A lost claim leaves the rest to the new owner, or stops loudly in Cancelling for recovery
            // rather than landing over an undo that never ran.
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
     * A level with a row a live worker may still hold is neither sent nor judged: the rollback
     * waits there to be taken over again.
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

    private function held(CompensationRun $compensation): bool
    {
        return $compensation->status === CompensationStatus::Running
            && ($compensation->reclaim_stale_at === null || $compensation->reclaim_stale_at->isFuture());
    }

    /**
     * A refused landing is raised: only the caller knows whether losing the run matters.
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
     * The owner token is replaced first, so the owner claims nothing more before what it left
     * undone is read.
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
     * The first compensation that failed, or else the first that never settled, in one read.
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
     * Not completed, not just failed: a row still Pending or Running lost its job or its worker.
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
