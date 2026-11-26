<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use Closure;
use DiscoveryUkraine\SagaLaraFlow\Concerns\NormalizesExceptions;
use DiscoveryUkraine\SagaLaraFlow\Contracts\ActionRunRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\RollbackNotPlannedException;
use DiscoveryUkraine\SagaLaraFlow\Jobs\ResumeWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Jobs\RunActionJob;
use DiscoveryUkraine\SagaLaraFlow\Jobs\RunWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Queue\Events\Looping;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Throwable;

/**
 * Batch-bound work (parallel actions, compensations) is out of reach even for R3 and a kick: adding
 * a job back into a Bus::batch needs an id this package does not store.
 */
final readonly class FlowDoctor
{
    use NormalizesExceptions;

    public function __construct(
        private FlowRepository $flowRepository,
        private ActionRunRepository $actionRunRepository,
        private ActionRecorder $actionRecorder,
        private FlowLifecycleRecorder $lifecycle,
        private FlowExecutor $executor,
    ) {}

    /**
     * @throws Throwable
     */
    public function repair(): FlowRepairReport
    {
        if (! config('saga-lara-flow.repair.enabled')) {
            return new FlowRepairReport;
        }

        $limit = (int) config('saga-lara-flow.repair.batch_size', 100);
        $grace = (int) config('saga-lara-flow.repair.grace_seconds', 60);
        $maxAttempts = (int) config('saga-lara-flow.repair.max_attempts', 10);

        [$lostActions, $skippedLost] = $this->applyRule(
            'redispatch_lost_actions',
            fn (): iterable => $this->actionRunRepository->dueForRepair($limit, $grace, $maxAttempts),
            fn (ActionRun $action): bool => $this->redispatchAction($action, $maxAttempts),
        );

        [$staleActions, $skippedStale] = $this->applyRule(
            'redispatch_stale_running_actions',
            fn (): iterable => $this->actionRunRepository->dueForStaleRunningRepair($limit, $maxAttempts),
            fn (ActionRun $action): bool => $this->redispatchStaleRunningAction($action, $maxAttempts),
        );

        [$rewokenFlows, $skippedFlows] = $this->applyRule(
            'wake_stuck_flows',
            fn (): iterable => $this->flowRepository->dueForRepair($limit, $grace, $maxAttempts),
            fn (FlowRun $run): bool => $this->rewake($run, FlowStatus::Waiting, 'lost_resume', $maxAttempts),
        );

        [$lostStarts, $skippedStarts] = $this->applyRule(
            'redispatch_lost_starts',
            fn (): iterable => $this->flowRepository->dueForStartRepair($limit, $grace, $maxAttempts),
            fn (FlowRun $run): bool => $this->rewake($run, FlowStatus::Pending, 'lost_start', $maxAttempts),
        );

        [$resumedRollbacks, $skippedRollbacks] = $this->applyRule(
            'resume_stalled_rollbacks',
            fn (): iterable => $this->flowRepository->dueForRollbackRepair($limit, $grace, $maxAttempts),
            fn (FlowRun $run): bool => $this->resumeRollback($run),
        );

        return new FlowRepairReport(
            $lostActions + $staleActions,
            $rewokenFlows + $lostStarts,
            $skippedLost + $skippedStale + $skippedFlows + $skippedStarts + $skippedRollbacks,
            $resumedRollbacks,
        );
    }

    /**
     * @template TEntity of ActionRun|FlowRun
     *
     * @param  Closure(): iterable<TEntity>  $candidates
     * @param  Closure(TEntity): bool  $repair
     * @return array{int, int}
     */
    private function applyRule(string $switch, Closure $candidates, Closure $repair): array
    {
        if (! config("saga-lara-flow.repair.{$switch}")) {
            return [0, 0];
        }

        $repaired = 0;
        $skipped = 0;

        foreach ($candidates() as $entity) {
            $repair($entity) ? $repaired++ : $skipped++;
        }

        return [$repaired, $skipped];
    }

    /**
     * @throws Throwable
     */
    private function redispatchAction(ActionRun $action, int $maxAttempts): bool
    {
        $confirmed = $this->connection()->transaction(function () use ($action, $maxAttempts): bool {
            $lockedAction = $this->lockAction($action->id);

            if (
                $lockedAction === null
                || $lockedAction->status !== ActionStatus::Pending
                || $lockedAction->parallel_group !== null
                || $lockedAction->repair_attempts >= $maxAttempts
                || ! $this->repairWindowOpen($lockedAction->repair_available_at)
            ) {
                return false;
            }

            // Never resurrect a step whose run is finished or rolling back.
            if (! $lockedAction->flowRun->status->canStartWork()) {
                return false;
            }

            $this->bumpThrottle($lockedAction);

            $this->actionRecorder->actionRedispatched($lockedAction);

            return true;
        });

        if ($confirmed) {
            $this->dispatchActionJob($action->fresh() ?? $action);
        }

        return $confirmed;
    }

    /**
     * @throws Throwable
     */
    private function redispatchStaleRunningAction(ActionRun $action, int $maxAttempts): bool
    {
        $confirmed = $this->connection()->transaction(function () use ($action, $maxAttempts): bool {
            $lockedAction = $this->lockAction($action->id);

            if (
                $lockedAction === null
                || $lockedAction->status !== ActionStatus::Running
                || $lockedAction->parallel_group !== null
                || $lockedAction->reclaim_stale_at === null
                || $lockedAction->reclaim_stale_at->isAfter(now())
                || $lockedAction->repair_attempts >= $maxAttempts
                || ! $this->repairWindowOpen($lockedAction->repair_available_at)
            ) {
                return false;
            }

            // Never resurrect a step whose run is finished or rolling back.
            if (! $lockedAction->flowRun->status->canStartWork()) {
                return false;
            }

            $this->bumpThrottle($lockedAction);

            $this->actionRecorder->actionRedispatched($lockedAction);

            return true;
        });

        if ($confirmed) {
            $this->dispatchActionJob($action->fresh() ?? $action);
        }

        return $confirmed;
    }

    /**
     * @throws Throwable
     */
    private function rewake(FlowRun $run, FlowStatus $stuckIn, string $reason, int $maxAttempts): bool
    {
        $confirmed = $this->connection()->transaction(function () use ($run, $stuckIn, $reason, $maxAttempts): bool {
            $locked = $this->lockFlow($run->id);

            if ($locked === null
                || $locked->status !== $stuckIn
                || $locked->repair_attempts >= $maxAttempts
                || ! $this->repairWindowOpen($locked->repair_available_at)) {
                return false;
            }

            $this->bumpThrottle($locked);

            $this->lifecycle->flowRewoken($locked, $reason);

            return true;
        });

        if ($confirmed) {
            $this->dispatchDrive($run->fresh() ?? $run, start: $stuckIn === FlowStatus::Pending);
        }

        return $confirmed;
    }

    /**
     * The budget is spent only by a takeover.
     *
     * @throws Throwable
     */
    private function resumeRollback(FlowRun $run): bool
    {
        try {
            $resumed = $this->executor->resumeRollback($run);
        } catch (RollbackNotPlannedException $failure) {
            $this->spendRollbackBudget($run);

            app(AnomalyLog::class)->log(AnomalyLog::REASON_ROLLBACK_RESUME_FAILED, [
                'entity' => 'flow',
                'flow_run_id' => $run->id,
                'workflow_class' => $run->workflow_class,
                'rollback_target' => $run->rollback_target?->value,
                'exception' => $this->exceptionToArray($failure->getPrevious() ?? $failure),
            ]);

            return false;
        }

        if ($resumed) {
            $this->spendRollbackBudget($run);
        }

        return $resumed;
    }

    /**
     * @throws Throwable
     */
    public function takeOverStalledRollback(FlowRun $run): bool
    {
        $since = Carbon::now()->subSeconds((int) config('saga-lara-flow.repair.grace_seconds', 60));

        $stalled = $run->newQuery()->useWritePdo()->whereKey($run->getKey())
            ->where(fn ($query) => $run::rollbackStalled($query, $since))
            ->whereNotNull('rollback_target')
            ->exists();

        if (! $stalled || ! $this->executor->resumeRollback($run)) {
            return false;
        }

        $run->newQuery()->whereKey($run->getKey())->update(['repair_attempts' => 0, 'repair_available_at' => null]);

        return true;
    }

    /**
     * Counted in the database: a second takeover during a long replay must not be written over.
     */
    private function spendRollbackBudget(FlowRun $run): void
    {
        $row = $run->newQuery()->whereKey($run->getKey());

        (clone $row)->increment('repair_attempts');

        $attempts = (int) (clone $row)->useWritePdo()->value('repair_attempts');

        (clone $row)->update(['repair_available_at' => now()->addSeconds($this->backoff($attempts))]);
    }

    /**
     * @throws Throwable
     */
    public function kick(FlowRun $run): FlowRun
    {
        $current = $this->rereadFlow($run);

        if ($current->status === FlowStatus::Cancelling) {
            return $this->takeOverStalledRollback($current) ? $this->rereadFlow($current) : $current;
        }

        if (! $current->status->canStartWork()) {
            return $current;
        }

        /** @var array{0: ?FlowRun, 1: ?ActionRun} $kicked */
        $kicked = $this->connection()->transaction(function () use ($current): array {
            $locked = $this->lockFlow($current->id);

            if ($locked === null || ! $locked->status->canStartWork()) {
                return [$locked, null];
            }

            $this->clearRepairBudget($locked);

            $step = $this->openSequentialStep($locked);

            // Last: a host listener that swallows a failing query leaves the connection unusable.
            $this->lifecycle->flowRewoken($locked, 'manual');

            return [$locked, $step];
        });

        [$locked, $step] = $kicked;

        if ($locked === null || ! $locked->status->canStartWork()) {
            return $locked ?? $current;
        }

        $kickedRun = $this->budgetSurvivedCommit($locked);

        $this->dispatchDrive($kickedRun, start: $locked->status === FlowStatus::Pending);

        if ($step !== null) {
            // Lazy-loading would ask a replica which connection and queue the recovery lands on.
            $step->setRelation('flowRun', $kickedRun);

            $this->dispatchActionJob($step);
        }

        return $kickedRun;
    }

    /**
     * Read back after the commit: the re-wake event runs host listeners inside the transaction.
     */
    private function budgetSurvivedCommit(FlowRun $run): FlowRun
    {
        $stored = $this->rereadFlow($run);

        if ($stored->repair_attempts === 0) {
            return $stored;
        }

        app(AnomalyLog::class)->log(AnomalyLog::REASON_CLAIM_NOT_COMMITTED, [
            'entity' => 'flow',
            'flow_run_id' => $run->id,
            'workflow_class' => $run->workflow_class,
            'status' => $stored->status->value,
            'stored_repair_attempts' => $stored->repair_attempts,
        ]);

        return $stored;
    }

    /**
     * With the hold retryAction() gives a rewound row: otherwise the doctor would read the job this
     * kick sends as lost and send a second.
     */
    private function clearRepairBudget(FlowRun $run): void
    {
        $heldUntil = Carbon::now()->addSeconds((int) config('saga-lara-flow.repair.grace_seconds', 60));

        $run->repair_attempts = 0;
        $run->repair_available_at = $heldUntil;
        $run->save();

        $this->actionRunModel()::query()
            ->where('flow_run_id', $run->id)
            ->whereIn('status', [
                ActionStatus::Pending,
                ActionStatus::Running,
                ActionStatus::AwaitingRetry,
            ])
            ->update(['repair_attempts' => 0, 'repair_available_at' => $heldUntil]);
    }

    /**
     * A Running row inside its reclaim window is skipped: a second job would wait on the live
     * worker's lock, and the queue giving up would be recorded against the row.
     */
    private function openSequentialStep(FlowRun $run): ?ActionRun
    {
        return $this->actionRunModel()::query()
            ->where('flow_run_id', $run->id)
            ->whereNull('parallel_group')
            ->where(function ($query): void {
                $query
                    ->where('status', ActionStatus::Pending)
                    ->orWhere(function ($stale): void {
                        $stale
                            ->where('status', ActionStatus::Running)
                            ->whereNotNull('reclaim_stale_at')
                            ->where('reclaim_stale_at', '<=', now());
                    });
            })
            ->orderBy('sequence')
            ->first();
    }

    private function rereadFlow(FlowRun $run): FlowRun
    {
        return $run->newQuery()->useWritePdo()->find($run->getKey()) ?? $run;
    }

    /**
     * @throws Throwable
     */
    public function onQueueLooping(Looping $event): void
    {
        if (! config('saga-lara-flow.repair.enabled')) {
            return;
        }

        $seconds = (int) config('saga-lara-flow.repair.queue_looping.throttle_seconds', 60);
        $prefix = (string) config('saga-lara-flow.locks.prefix', 'saga-lara-flow');

        if (Cache::lock($prefix.':repair-loop', $seconds)->get()) {
            $this->repair();
        }
    }

    /**
     * The package's connection, so the repair transaction and its row lock reach a dedicated one.
     */
    private function connection(): ConnectionInterface
    {
        return DB::connection(config('saga-lara-flow.database.connection') ?: null);
    }

    private function lockAction(string $id): ?ActionRun
    {
        return $this->actionRunModel()::query()->lockForUpdate()->find($id);
    }

    /**
     * @return class-string<ActionRun>
     */
    private function actionRunModel(): string
    {
        /** @var class-string<ActionRun> $model */
        $model = config('saga-lara-flow.models.action_run');

        return $model;
    }

    private function lockFlow(string $id): ?FlowRun
    {
        /** @var class-string<FlowRun> $model */
        $model = config('saga-lara-flow.models.flow_run');

        return $model::query()->lockForUpdate()->find($id);
    }

    private function repairWindowOpen(?\DateTimeInterface $availableAt): bool
    {
        return $availableAt === null || $availableAt <= now();
    }

    private function bumpThrottle(ActionRun|FlowRun $entity): void
    {
        $attempts = $entity->repair_attempts + 1;

        $entity->repair_attempts = $attempts;
        $entity->repair_available_at = now()->addSeconds($this->backoff($attempts));
        $entity->save();
    }

    private function backoff(int $attempts): int
    {
        $base = (int) config('saga-lara-flow.repair.backoff.base_seconds', 10);
        $max = (int) config('saga-lara-flow.repair.backoff.max_seconds', 300);

        return min($max, $base * (2 ** max(0, $attempts - 1)));
    }

    private function dispatchActionJob(ActionRun $action): void
    {
        $flow = $action->flowRun;

        $job = RunActionJob::dispatch($action->id, $action->action_class, $action->retry_signal_attempts);

        if ($flow->connection !== null) {
            $job->onConnection($flow->connection);
        }

        if ($flow->queue !== null) {
            $job->onQueue($flow->queue);
        }

        if (config('saga-lara-flow.queue.after_commit')) {
            $job->afterCommit();
        }
    }

    /**
     * A run not yet started gets its first job again: only that class waits on a late original's lock.
     */
    private function dispatchDrive(FlowRun $run, bool $start): void
    {
        $job = $start
            ? RunWorkflowJob::dispatch($run->id, startOnly: true)
            : ResumeWorkflowJob::dispatch($run->id);

        if ($run->connection !== null) {
            $job->onConnection($run->connection);
        }

        if ($run->queue !== null) {
            $job->onQueue($run->queue);
        }

        if (config('saga-lara-flow.queue.after_commit')) {
            $job->afterCommit();
        }
    }
}
