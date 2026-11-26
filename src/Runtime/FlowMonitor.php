<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Concerns\NormalizesExceptions;
use DiscoveryUkraine\SagaLaraFlow\Contracts\ActionRunRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\SignalRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ExpirationNotPlannedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowExpiredException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\RollbackNotPlannedException;
use DiscoveryUkraine\SagaLaraFlow\Jobs\ResumeWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;
use Illuminate\Database\Query\Builder;
use Illuminate\Queue\Events\Looping;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Cache;
use Throwable;

/**
 * Every write moves the entity out of what its scan filters on, so re-running sweep() is idempotent.
 */
final readonly class FlowMonitor
{
    use NormalizesExceptions;

    public function __construct(
        private FlowRepository $flows,
        private ActionRunRepository $actions,
        private SignalRepository $signals,
        private FlowExecutor $executor,
        private SignalRecorder $signalRecorder,
        private ActionRecorder $actionRecorder,
    ) {}

    /**
     * @return array{runs: int, signals: int, actions: int, rollbacks: int}
     *
     * @throws Throwable
     */
    public function sweep(): array
    {
        if (! config('saga-lara-flow.monitor.expiration.enabled')) {
            return ['runs' => 0, 'signals' => 0, 'actions' => 0, 'rollbacks' => 0];
        }

        $limit = (int) config('saga-lara-flow.monitor.expiration.batch_size', 100);

        // Runs first: an expired run leaves Waiting, so the passes below skip its signals and steps.
        return [
            'runs' => $this->expireRuns($limit),
            'signals' => $this->timeoutSignals($limit),
            'actions' => $this->expireActions($limit),
            'rollbacks' => $this->resumeRollbacks($limit),
        ];
    }

    /**
     * The lock is never released: its TTL is the throttle window.
     *
     * @throws Throwable
     */
    public function onQueueLooping(Looping $event): void
    {
        if (! config('saga-lara-flow.monitor.enabled')) {
            return;
        }

        $seconds = (int) config('saga-lara-flow.monitor.queue_looping.throttle_seconds', 30);
        $prefix = (string) config('saga-lara-flow.locks.prefix', 'saga-lara-flow');

        if (Cache::lock($prefix.':monitor-loop', $seconds)->get()) {
            $this->sweep();
        }
    }

    /**
     * @throws Throwable
     */
    private function expireRuns(int $limit): int
    {
        $count = 0;

        foreach ($this->flows->dueForExpiration($limit) as $run) {
            if ($this->expireRun($run)) {
                $count++;
            }
        }

        return $count;
    }

    /**
     * Only a plan that fails before the move is stepped over: thrown, it would come back every sweep
     * and starve the passes below. Anything later would hide a rollback that started.
     */
    private function expireRun(FlowRun $run): bool
    {
        try {
            $this->executor->expireRun($run);

            return true;
        } catch (ExpirationNotPlannedException $failure) {
            $this->holdOff($run);

            app(AnomalyLog::class)->log(AnomalyLog::REASON_EXPIRY_FAILED, [
                'entity' => 'flow',
                'flow_run_id' => $run->id,
                'workflow_class' => $run->workflow_class,
                'status' => $run->status->value,
                'expiry_attempts' => $run->expiry_attempts,
                'exception' => $this->exceptionToArray($failure->getPrevious() ?? $failure),
            ]);

            return false;
        }
    }

    /**
     * @throws Throwable
     */
    private function resumeRollbacks(int $limit): int
    {
        $grace = (int) config('saga-lara-flow.monitor.expiration.rollback_grace_seconds', 60);

        $count = 0;

        foreach ($this->flows->dueForRollbackRecovery($limit, $grace) as $run) {
            if ($this->resumeRollback($run)) {
                $count++;
            }
        }

        return $count;
    }

    /**
     * A takeover is held off too: on a queue further behind than the grace window, taking over on
     * every pass would leave each new owner's jobs stale before a worker reached them.
     *
     * @throws Throwable
     */
    private function resumeRollback(FlowRun $run): bool
    {
        try {
            $resumed = $this->executor->resumeRollback($run);
        } catch (RollbackNotPlannedException $failure) {
            $this->holdOff($run);

            app(AnomalyLog::class)->log(AnomalyLog::REASON_ROLLBACK_RESUME_FAILED, [
                'entity' => 'flow',
                'flow_run_id' => $run->id,
                'workflow_class' => $run->workflow_class,
                'rollback_target' => $run->rollback_target?->value,
                'expiry_attempts' => $run->expiry_attempts,
                'exception' => $this->exceptionToArray($failure->getPrevious() ?? $failure),
            ]);

            return false;
        }

        if ($resumed) {
            $this->holdOff($run);
        }

        return $resumed;
    }

    /**
     * A plain statement: no Eloquent events, updated_at (the doctor's clock) untouched. Fenced on the
     * window still being open, so two sweeps cannot stack a hold-off.
     */
    private function holdOff(FlowRun $run): void
    {
        $now = Carbon::now();

        $attempts = $this->recordedAttempts($run) + 1;

        $written = $run->newQuery()->toBase()
            ->where($run->getKeyName(), $run->getKey())
            ->where(function (Builder $query) use ($now): void {
                $query->whereNull('expiry_available_at')
                    ->orWhere('expiry_available_at', '<=', $now);
            })
            ->update([
                'expiry_attempts' => $attempts,
                'expiry_available_at' => $now->copy()->addSeconds($this->backoff($attempts)),
            ]);

        // Refused: the journal names the count the sweep that got there first wrote.
        $run->expiry_attempts = $written === 1 ? $attempts : $this->recordedAttempts($run);
    }

    private function recordedAttempts(FlowRun $run): int
    {
        return (int) $run->newQuery()->toBase()->useWritePdo()
            ->where($run->getKeyName(), $run->getKey())
            ->value('expiry_attempts');
    }

    private function backoff(int $attempts): int
    {
        $base = (int) config('saga-lara-flow.monitor.expiration.backoff.base_seconds', 60);
        $max = (int) config('saga-lara-flow.monitor.expiration.backoff.max_seconds', 3600);

        return min($max, $base * (2 ** max(0, $attempts - 1)));
    }

    private function timeoutSignals(int $limit): int
    {
        $count = 0;

        foreach ($this->signals->dueForTimeout($limit) as $signal) {
            if ($this->timeoutSignal($signal)) {
                $count++;
            }
        }

        return $count;
    }

    private function timeoutSignal(FlowSignal $signal): bool
    {
        $run = $this->flows->find($signal->flow_run_id);

        if ($run === null || $run->isTerminal()) {
            return false;
        }

        // Lost to a delivery since the read: its own replay resolves the wait.
        if (! $this->signalRecorder->timeoutSignal($signal)) {
            return false;
        }

        $this->wake($run);

        return true;
    }

    private function expireActions(int $limit): int
    {
        $count = 0;

        foreach ($this->actions->dueForExpiration($limit) as $action) {
            if ($this->expireAction($action)) {
                $count++;
            }
        }

        return $count;
    }

    private function expireAction(ActionRun $action): bool
    {
        $run = $this->flows->find($action->flow_run_id);

        if ($run === null || $run->isTerminal()) {
            return false;
        }

        $expired = $this->actionRecorder->expireAction(
            $action,
            $this->exceptionToArray(FlowExpiredException::forAction($action->action_class, $action->sequence)),
        );

        if (! $expired) {
            return false;
        }

        $this->wake($run);

        return true;
    }

    private function wake(FlowRun $run): void
    {
        if ($run->status !== FlowStatus::Waiting) {
            return;
        }

        $dispatch = ResumeWorkflowJob::dispatch($run->id);

        if ($run->connection !== null) {
            $dispatch->onConnection($run->connection);
        }

        if ($run->queue !== null) {
            $dispatch->onQueue($run->queue);
        }

        if (config('saga-lara-flow.queue.after_commit')) {
            $dispatch->afterCommit();
        }
    }
}
