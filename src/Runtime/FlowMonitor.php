<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Concerns\NormalizesExceptions;
use DiscoveryUkraine\SagaLaraFlow\Contracts\ActionRunRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\SignalRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowExpiredException;
use DiscoveryUkraine\SagaLaraFlow\Jobs\ResumeWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;
use Illuminate\Queue\Events\Looping;
use Illuminate\Support\Facades\Cache;
use Throwable;

/**
 * The opt-in expiration sweep (§15) — the package has no durable timers, so a
 * scheduled command (saga-flow:monitor) or an optional throttled queue-looping
 * listener calls sweep() periodically. One pass expires overdue runs, times out
 * stuck signal waits, and expires overdue action steps, each capped at the
 * configured batch size.
 *
 * Every transition the sweep performs moves the entity out of the status its scan
 * filters on (a run leaves Running/Waiting, a wait-marker Waiting→TimedOut, an
 * action Pending/Running→Expired), so re-running sweep() is idempotent.
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
     * Run one expiration pass. Returns the number of entities acted on per category.
     *
     * @return array{runs: int, signals: int, actions: int}
     *
     * @throws Throwable
     */
    public function sweep(): array
    {
        if (! config('saga-lara-flow.monitor.expiration.enabled')) {
            return ['runs' => 0, 'signals' => 0, 'actions' => 0];
        }

        $limit = (int) config('saga-lara-flow.monitor.expiration.batch_size', 100);

        // Runs first: a run expired here leaves Waiting, so its own signals/actions
        // are skipped by the two passes below (we never wake a run mid-rollback).
        return [
            'runs' => $this->expireRuns($limit),
            'signals' => $this->timeoutSignals($limit),
            'actions' => $this->expireActions($limit),
        ];
    }

    /**
     * Throttled queue-worker hook (opt-in via config). Acquires a cache lock with a
     * TTL equal to the throttle window and never releases it, so at most one sweep
     * runs per window regardless of how many workers fire the Looping event.
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
            $this->expireRun($run);

            $count++;
        }

        return $count;
    }

    /**
     * Expire an overdue run. The mechanism (rebuild the compensation stack, roll it
     * back queued, finalize as Expired) lives on FlowExecutor — the owner of every
     * run-terminal transition — and is shared with the lazy drive() deadline check.
     *
     * @throws Throwable
     */
    private function expireRun(FlowRun $run): void
    {
        $this->executor->expireRun($run);
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

        $this->signalRecorder->timeoutSignal($signal);

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

        $this->actionRecorder->expireAction(
            $action,
            $this->exceptionToArray(FlowExpiredException::forAction($action->action_class, $action->sequence)),
        );

        $this->wake($run);

        return true;
    }

    /**
     * Resume a still-waiting run so it replays and resolves the just-changed marker
     * (a timed-out signal throws, an expired action fails or returns its fallback).
     * A run mid-rollback (Cancelling) or otherwise not parked is left untouched.
     */
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
