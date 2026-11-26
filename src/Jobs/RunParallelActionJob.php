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
use Illuminate\Database\Eloquent\Relations\Relation;
use Illuminate\Foundation\Queue\Queueable;
use Throwable;

/**
 * Does not resume the workflow: the batch's finally callback or the sending pass drives the join.
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
        if ($this->batch()?->cancelled()) {
            return;
        }

        $action = $this->resolveAction();

        if ($action === null) {
            return;
        }

        if ($dispatcher->execute($action, expectedRetryGeneration: 0)->settled()) {
            $this->wakeIfTheBatchClosedWithoutUs($action);
        }
    }

    /**
     * A batch finished while this job is still in handle() means a duplicate delivery drove the count
     * to zero early: the finally callback already fired, before this step settled, and will not again.
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

        if ($action->continue_on_failure && $action->status === ActionStatus::Failed) {
            app(ActionRecorder::class)->optionalFail($action);

            return;
        }

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

        return $model::query()
            ->useWritePdo()
            ->with(['flowRun' => fn (Relation $run) => $run->useWritePdo()])
            ->find($this->actionRunId);
    }
}
