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

        $this->dispatchLevel($flowRun, $levels, $primary, $finalState);
    }

    /**
     * Batch-completion continuation (queued mode): inspect the level that just ran,
     * halt on a Stop-policy failure, otherwise dispatch the next level or finalize.
     *
     * @param  list<list<CompensationEntry>>  $remainingLevels
     * @param  array<string, mixed>|null  $primary
     * @param  list<string>  $ranCompensationIds
     *
     * @throws Throwable
     */
    public function advance(
        string $flowRunId,
        array $remainingLevels,
        ?array $primary,
        FlowStatus $finalState,
        array $ranCompensationIds
    ): void {
        $flowRun = $this->repository->find($flowRunId);

        if ($flowRun === null) {
            return;
        }

        try {
            if ($this->levelStopped($ranCompensationIds)) {
                $this->finalize($flowRun, $finalState, $primary);

                return;
            }

            $this->dispatchLevel($flowRun, $remainingLevels, $primary, $finalState);
        } catch (ConcurrentFlowTransitionException) {
            // Another actor finished the run while the compensations were in flight; what
            // they ran is recorded either way. A batch callback is the one landing with no
            // caller to tell, so it absorbs here rather than in finalize().
        }
    }

    /**
     * @param  list<list<CompensationEntry>>  $levels
     * @param  array<string, mixed>|null  $primary
     */
    private function rollbackSync(FlowRun $flowRun, array $levels, ?array $primary, FlowStatus $finalState): void
    {
        foreach ($levels as $level) {
            if ($this->runLevelSync($flowRun, $level)) {
                $this->finalize($flowRun, $finalState, $primary);

                return;
            }
        }

        $this->finalize($flowRun, $finalState, $primary);
    }

    /**
     * @param  list<CompensationEntry>  $level
     * @return bool whether a Stop-policy compensation failed (rollback must halt)
     *
     * @throws Throwable
     */
    private function runLevelSync(FlowRun $flowRun, array $level): bool
    {
        $compensationIds = [];

        foreach ($level as $entry) {
            $compensation = $this->recorder->register($flowRun, $entry, $entry->effectivePolicy());

            $compensationIds[] = $compensation->id;

            // The row was just created above, in this same call: nothing else can have
            // taken it, so losing the claim is a broken invariant rather than a race.
            // Nothing supersedes a compensation mid-run either — they carry no expiry
            // sweep and the doctor leaves them alone — but the two are kept apart here
            // so this stays an invariant guard and not a catch-all.
            if ($this->executor->execute($compensation, $entry->definition) === StepExecution::ClaimLost) {
                throw ActionClaimFailedException::forCompensation(
                    $entry->definition->class ?? $entry->definition->type,
                    $compensation->sequence,
                );
            }
        }

        return $this->levelStopped($compensationIds);
    }

    /**
     * @param  list<list<CompensationEntry>>  $levels
     * @param  array<string, mixed>|null  $primary
     *
     * @throws Throwable
     */
    private function dispatchLevel(FlowRun $flowRun, array $levels, ?array $primary, FlowStatus $finalState): void
    {
        if ($levels === []) {
            $this->finalize($flowRun, $finalState, $primary);

            return;
        }

        $head = array_shift($levels);

        $compensationIds = [];

        $jobs = [];

        foreach ($head as $entry) {
            $compensation = $this->recorder->register($flowRun, $entry, $entry->effectivePolicy());

            $compensationIds[] = $compensation->id;

            $jobs[] = new RunCompensationJob($flowRun->id, $compensation->id, $entry->definition);
        }

        $batch = Bus::batch($jobs)
            ->name("saga-compensate:{$flowRun->id}")
            ->allowFailures()
            ->finally(new AdvanceCompensation($flowRun->id, $levels, $primary, $finalState, $compensationIds));

        if ($flowRun->connection !== null) {
            $batch->onConnection($flowRun->connection);
        }

        if ($flowRun->queue !== null) {
            $batch->onQueue($flowRun->queue);
        }

        $batch->dispatch();
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

        $failed = $this->firstFailedCompensation($flowRun);

        if ($failed !== null) {
            $exception['compensation'] = $this->exceptionToArray(CompensationFailedException::for($failed))
                + ['cause' => $failed->exception];
        } elseif (($unfinished = $this->firstUnfinishedCompensation($flowRun)) !== null) {
            // Recorded in the same place a failure would be, so a run that did not
            // fully unwind never looks like one that did.
            $exception['compensation'] = $this->exceptionToArray(
                CompensationUnfinishedException::for($unfinished)
            );
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
            ->whereIn('id', $compensationIds)
            ->whereNot('status', CompensationStatus::Completed)
            ->where('continue_on_failure', false)
            ->exists();
    }

    private function firstFailedCompensation(FlowRun $flowRun): ?CompensationRun
    {
        return $this->compensationModel()->newQuery()
            ->where('flow_run_id', $flowRun->id)
            ->where('status', CompensationStatus::Failed)
            ->orderBy('sequence')
            ->first();
    }

    /**
     * The earliest compensation that never reached a terminal state. Reported only
     * when no compensation actually failed — a real failure carries its own cause and
     * is the more useful thing to surface.
     */
    private function firstUnfinishedCompensation(FlowRun $flowRun): ?CompensationRun
    {
        return $this->compensationModel()->newQuery()
            ->where('flow_run_id', $flowRun->id)
            ->whereIn('status', [CompensationStatus::Pending, CompensationStatus::Running])
            ->orderBy('sequence')
            ->first();
    }

    private function compensationModel(): CompensationRun
    {
        /** @var class-string<CompensationRun> $model */
        $model = config('saga-lara-flow.models.compensation_run');

        return new $model;
    }
}
