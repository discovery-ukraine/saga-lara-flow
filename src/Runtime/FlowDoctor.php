<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use Closure;
use DiscoveryUkraine\SagaLaraFlow\Contracts\ActionRunRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Jobs\ResumeWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Jobs\RunActionJob;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Queue\Events\Looping;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Throwable;

/**
 * The opt-in repair pass — the "doctor". Distinct from the expiration monitor
 * (FlowMonitor): it recovers progress lost to a *dropped job*, not a passed deadline. It only ever
 * re-dispatches an existing job or re-wakes a run, so it writes no step and no outcome of its own
 * and cannot duplicate an ordinal — the run carries on normally from there, scheduling whatever
 * comes next. R3 can still start a second execution of one step, since a reclaim window expiring
 * does not prove the first worker is dead.
 *
 *   R1  a stuck sequential Pending action (its RunActionJob was lost) → re-dispatch.
 *   R2  a stuck Waiting run with nothing in flight (its resume was lost) → re-wake.
 *   R3  a stuck sequential Running action past its own reclaim window → re-dispatch.
 *
 * repair_attempts + repair_available_at throttle a pass per entity and give up after max_attempts.
 * saga-flow:kick is the manual answer to all three: it refills that budget for the run and its
 * unfinished steps and sends the sequential step its own job back. Batch-bound work (parallel
 * actions, compensations) is out of scope even for R3 and for a kick: adding a job back into a
 * Bus::batch needs an id this package does not store.
 */
final readonly class FlowDoctor
{
    public function __construct(
        private FlowRepository $flowRepository,
        private ActionRunRepository $actionRunRepository,
        private ActionRecorder $actionRecorder,
        private FlowLifecycleRecorder $lifecycle,
    ) {}

    /**
     * Run one repair pass. Returns counts per category.
     *
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
            fn (FlowRun $run): bool => $this->wakeWaiting($run, $maxAttempts),
        );

        return new FlowRepairReport(
            $lostActions + $staleActions,
            $rewokenFlows,
            $skippedLost + $skippedStale + $skippedFlows,
        );
    }

    /**
     * Run one repair rule if its config switch is on, returning [repaired, skipped].
     * Candidates come from a closure so a disabled rule never queries for them.
     *
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
     * R1: re-dispatch a lost RunActionJob for a stuck sequential Pending action.
     * Re-verifies the candidate under a row lock, bumps the throttle, records the
     * intervention, then dispatches the fresh job after the transaction commits.
     *
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
     * R3: re-dispatch a fresh RunActionJob for a stuck sequential Running action past
     * its own reclaim window (actions.reclaim.stale_running). Re-verifies staleness
     * under a row lock — the row may have been reclaimed and restarted since the
     * candidate was read — bumps the throttle, records the intervention, then
     * dispatches after the transaction commits. Like R1 it only sends a job: the fresh
     * job's own claim decides whether it wins the row.
     *
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
     * R2: re-wake a stuck Waiting run whose resume was lost. Re-verifies it is still
     * Waiting under a row lock, bumps the throttle, records the re-wake, then
     * dispatches ResumeWorkflowJob after commit so replay decides what happens next.
     *
     * @throws Throwable
     */
    private function wakeWaiting(FlowRun $run, int $maxAttempts): bool
    {
        $confirmed = $this->connection()->transaction(function () use ($run, $maxAttempts): bool {
            $locked = $this->lockFlow($run->id);

            if ($locked === null
                || $locked->status !== FlowStatus::Waiting
                || $locked->repair_attempts >= $maxAttempts
                || ! $this->repairWindowOpen($locked->repair_available_at)) {
                return false;
            }

            $this->bumpThrottle($locked);

            $this->lifecycle->flowRewoken($locked, 'lost_resume');

            return true;
        });

        if ($confirmed) {
            $this->dispatchResume($run->fresh() ?? $run);
        }

        return $confirmed;
    }

    /**
     * Manually re-drive a specific run (saga-flow:kick / the repair endpoint). Unlike
     * the automatic pass, this is unthrottled and ignores positive-evidence — a human
     * decided the run is stuck. Works for Pending/Waiting/Running (a same-state
     * Running transition is an idempotent no-op and the run lock serializes against
     * any live job); a run that may not start work is left untouched.
     *
     * The resume alone cannot move a run parked on a recorded step: the replay it
     * triggers meets that row and parks on it exactly as the pass that recorded it did.
     *
     * Decided on the writer, and the run it decided on is what comes back: a lagging
     * replica would answer with the status this read exists to replace, and an operator
     * would be told a run was re-driven while it was rolling back.
     *
     * @throws Throwable
     */
    public function kick(FlowRun $run): FlowRun
    {
        $current = $this->rereadFlow($run);

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

            // Last of all: the re-wake event runs host listeners, and one that swallows
            // a failing query leaves the connection unable to answer anything more.
            $this->lifecycle->flowRewoken($locked, 'manual');

            return [$locked, $step];
        });

        [$locked, $step] = $kicked;

        if ($locked === null || ! $locked->status->canStartWork()) {
            return $locked ?? $current;
        }

        $kickedRun = $this->budgetSurvivedCommit($locked);

        $this->dispatchResume($kickedRun);

        if ($step !== null) {
            // The writer already answered with this run; lazy-loading the relation
            // would ask a replica which connection and queue the recovery lands on.
            $step->setRelation('flowRun', $kickedRun);

            $this->dispatchActionJob($step);
        }

        return $kickedRun;
    }

    /**
     * The run as the writer holds it once the kick's transaction has closed. Mirrors
     * ActionRecorder::claimSurvivedCommit(): a commit reporting success is not proof,
     * and the re-wake event runs host listeners inside this one. Both jobs are sent
     * either way — neither reads what it wrote — but a caller handed a model claiming a
     * budget the database never took would read the run as reachable again.
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
     * Refill the budget the automatic pass spent on this run and on the steps it has
     * not finished. A kick is somebody watching, which is what max_attempts stands in
     * for while nobody is.
     */
    private function clearRepairBudget(FlowRun $run): void
    {
        $run->repair_attempts = 0;
        $run->repair_available_at = null;
        $run->save();

        $this->actionRunModel()::query()
            ->where('flow_run_id', $run->id)
            ->whereIn('status', [
                ActionStatus::Pending,
                ActionStatus::Running,
                ActionStatus::AwaitingRetry,
            ])
            ->update(['repair_attempts' => 0, 'repair_available_at' => null]);
    }

    /**
     * The step a kick can put back on the queue itself: the rows R1 and R3 read, without
     * their throttle.
     *
     * A Running row still inside its reclaim window is skipped rather than left to lose
     * the claim: a second job for a row a live worker holds waits on that worker's lock,
     * and the queue giving up on the wait is recorded against the row it is running.
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
     * Throttled queue-worker hook (opt-in via repair.queue_looping). Uses a
     * lock separate from the expiration sweep so repair loops independently.
     *
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
     * The package's configured database connection (matching UsesSagaFlowConnection),
     * so the repair transaction and its row lock target the right database even when
     * the package lives on a dedicated connection.
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

    /**
     * Record one repair attempt and schedule the next one with exponential backoff,
     * so a re-dispatch/re-wake is not retried every pass.
     */
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

    private function dispatchResume(FlowRun $run): void
    {
        $job = ResumeWorkflowJob::dispatch($run->id);

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
