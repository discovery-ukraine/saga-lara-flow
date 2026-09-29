<?php

namespace DiscoveryUkraine\SagaLaraFlow\Jobs;

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\ParallelFailurePolicy;
use DiscoveryUkraine\SagaLaraFlow\Middleware\LockMiddlewareFactory;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionDispatcher;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionRecorder;
use DiscoveryUkraine\SagaLaraFlow\Runtime\AnomalyLog;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowResumer;
use Illuminate\Bus\Batchable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use Throwable;

/**
 * Executes one step of a parallel() block as part of a Bus::batch. Unlike
 * RunActionJob it does NOT resume the workflow itself — the batch's finally
 * callback (ResumeParallelBlock) drives the single join after every step settles.
 *
 * On final failure: an optional step is recorded OptionalFailed (it never fails the
 * flow); a hard failure under the FailFast policy cancels the batch so pending
 * siblings never start (in-flight ones still finish — they cannot be force-killed).
 */
class RunParallelActionJob implements ShouldQueue
{
    use Batchable;
    use Queueable;

    public int $tries;

    public int $timeout;

    public function __construct(
        public string $actionRunId,
        public string $actionClass,
        public ParallelFailurePolicy $policy,
    ) {
        $defaults = get_class_vars($this->actionClass);

        $this->tries = isset($defaults['tries']) ? (int) $defaults['tries'] : 1;
        $this->timeout = isset($defaults['timeout']) ? (int) $defaults['timeout'] : 0;
    }

    /**
     * @throws Throwable
     */
    public function handle(ActionDispatcher $dispatcher): void
    {
        // A sibling's FailFast cancellation: skip starting this step.
        if ($this->batch()?->cancelled()) {
            return;
        }

        $action = $this->resolveAction();

        if ($action === null) {
            return;
        }

        // The claim inside execute() (ActionRecorder::startAction()) is the single
        // source of truth: a step already settled out of band (Completed earlier, or
        // Expired by the monitor) simply fails the claim and nothing runs. Parallel
        // steps carry no retry-on-signal cycle today (0 always), but passing it
        // explicitly keeps the guard correct if that ever changes.
        if ($dispatcher->execute($action, expectedRetryGeneration: 0)->settled()) {
            $this->wakeIfTheBatchClosedWithoutUs($action);
        }
    }

    /**
     * A batch cannot normally be finished while one of its own jobs is still inside
     * handle() — the pending count only drops once this returns. Finding it finished
     * means a duplicate delivery lost its claim, returned quietly, and drove the count
     * to zero early. ResumeParallelBlock already fired then, and Laravel will not fire
     * it again: its condition is (pendingJobs - failedJobs) === 0, and the next
     * decrement takes the count to -1.
     *
     * The join therefore replayed while this step was still Running and parked the run
     * with no wake left. An extra resume costs a replay that resolves to the same
     * history; a missing one costs a run that hangs until repair.wake_stuck_flows.
     */
    private function wakeIfTheBatchClosedWithoutUs(ActionRun $action): void
    {
        $batch = $this->batch();

        if ($batch === null || ! $batch->finished()) {
            return;
        }

        app(AnomalyLog::class)->log(AnomalyLog::REASON_BATCH_FINISHED_EARLY, [
            'entity' => 'parallel_action',
            'flow_run_id' => $action->flow_run_id,
            'action_run_id' => $action->id,
            'sequence' => $action->sequence,
            'action_class' => $action->action_class,
            'batch_id' => $batch->id,
        ]);

        app(FlowResumer::class)->resume($action->flowRun);
    }

    public function failed(Throwable $exception): void
    {
        $action = $this->resolveAction();

        if ($action === null) {
            return;
        }

        // Optional step exhausted its retries: record OptionalFailed, do not fail
        // the block (and never cancel the batch).
        if ($action->continue_on_failure && $action->status === ActionStatus::Failed) {
            app(ActionRecorder::class)->optionalFail($action);

            return;
        }

        // Hard failure under FailFast: cancel so pending siblings never start.
        if ($this->policy === ParallelFailurePolicy::FailFast) {
            $this->batch()?->cancel();
        }
    }

    /**
     * @return array<int, object>
     */
    public function middleware(): array
    {
        return app(LockMiddlewareFactory::class)->actionMiddleware($this->actionRunId);
    }

    private function resolveAction(): ?ActionRun
    {
        /** @var class-string<ActionRun> $model */
        $model = config('saga-lara-flow.models.action_run');

        return $model::query()->find($this->actionRunId);
    }
}
