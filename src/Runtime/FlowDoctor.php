<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

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
 * The opt-in repair pass (§15, Phase 8.2) — the "doctor". Distinct from the
 * expiration monitor (FlowMonitor): it recovers progress lost to a *dropped job*,
 * not a passed deadline. It is configured, scheduled, and looped independently
 * (config repair.*, command saga-flow:repair, its own queue-looping lock).
 *
 * It only ever re-dispatches an existing job or re-wakes a flow — never creating
 * duplicate work or mutating a business result. Two automatic cases, both safe
 * under repeated passes thanks to the jobs' own idempotency (skip-set on settled
 * steps, terminal-run guard) and replay:
 *
 *   R1  a stuck sequential Pending action (its RunActionJob was lost) → re-dispatch.
 *   R2  a stuck Waiting run with nothing in flight (its resume was lost) → re-wake;
 *       replay then advances it or parks it again.
 *
 * Each repaired entity carries repair_attempts + repair_available_at so a pass is
 * throttled per entity (exponential backoff) and gives up after max_attempts —
 * saga-flow:kick is the manual escape hatch. Batch-bound work (compensations,
 * parallel actions) and automatic Running re-drive are deliberately out of scope.
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

        $redispatchedActions = 0;
        $rewokenFlows = 0;
        $skipped = 0;

        if (config('saga-lara-flow.repair.redispatch_actions')) {
            foreach ($this->actionRunRepository->dueForRepair($limit, $grace, $maxAttempts) as $action) {
                $this->redispatchAction($action, $maxAttempts) ? $redispatchedActions++ : $skipped++;
            }
        }

        if (config('saga-lara-flow.repair.wake_waiting')) {
            foreach ($this->flowRepository->dueForRepair($limit, $grace, $maxAttempts) as $run) {
                $this->wakeWaiting($run, $maxAttempts) ? $rewokenFlows++ : $skipped++;
            }
        }

        return new FlowRepairReport($redispatchedActions, $rewokenFlows, $skipped);
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

            $flow = $lockedAction->flowRun;

            // Never resurrect a step whose run is finished or rolling back.
            if ($flow->isTerminal() || $flow->status === FlowStatus::Cancelling) {
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
     * any live job); a terminal run is left untouched.
     */
    public function kick(FlowRun $run): FlowRun
    {
        if ($run->isTerminal()) {
            return $run;
        }

        $this->lifecycle->flowRewoken($run, 'manual');

        $this->dispatchResume($run);

        return $run;
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
        /** @var class-string<ActionRun> $model */
        $model = config('saga-lara-flow.models.action_run');

        return $model::query()->lockForUpdate()->find($id);
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

        $job = RunActionJob::dispatch($action->id, $action->action_class);

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
