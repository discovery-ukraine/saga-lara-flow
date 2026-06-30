<?php

namespace DiscoveryUkraine\SagaLaraFlow\Jobs;

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\ParallelFailurePolicy;
use DiscoveryUkraine\SagaLaraFlow\Middleware\LockMiddlewareFactory;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionDispatcher;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionRecorder;
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

        if ($action->status !== ActionStatus::Completed) {
            $dispatcher->execute($action);
        }
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
