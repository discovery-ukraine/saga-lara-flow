<?php

namespace DiscoveryUkraine\SagaLaraFlow\Jobs;

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Middleware\LockMiddlewareFactory;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionDispatcher;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionRecorder;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use Throwable;

/**
 * Executes a single scheduled action step, then resumes its workflow so the
 * drive loop can replay and move on. Carries the action's native $tries/$timeout
 * so Laravel's queue retry semantics apply. On final failure it still resumes
 * the workflow, so the failure surfaces as a business error on replay.
 */
class RunActionJob implements ShouldQueue
{
    use Queueable;

    public int $tries;

    public int $timeout;

    public function __construct(
        public string $actionRunId,
        public string $actionClass,
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
        $action = $this->resolveAction();

        if ($action === null) {
            return;
        }

        // Skip a step already settled out of band: Completed on an earlier attempt,
        // or Expired by the monitor (a late job must not resurrect an expired step).
        if (! in_array($action->status, [ActionStatus::Completed, ActionStatus::Expired], true)) {
            $dispatcher->execute($action);
        }

        $this->resumeWorkflow($action);
    }

    public function failed(Throwable $exception): void
    {
        $action = $this->resolveAction();

        if ($action === null) {
            return;
        }

        // Optional step exhausted its retries: record it as OptionalFailed so the
        // workflow resolves the fallback instead of failing on replay.
        if ($action->continue_on_failure && $action->status === ActionStatus::Failed) {
            app(ActionRecorder::class)->optionalFail($action);
        }

        $this->resumeWorkflow($action);
    }

    /**
     * @return array<int, object>
     */
    public function middleware(): array
    {
        return app(LockMiddlewareFactory::class)->actionMiddleware($this->actionRunId);
    }

    private function resumeWorkflow(ActionRun $action): void
    {
        $flowRun = $action->flowRun;

        $job = ResumeWorkflowJob::dispatch($action->flow_run_id);

        if ($flowRun->connection !== null) {
            $job->onConnection($flowRun->connection);
        }

        if ($flowRun->queue !== null) {
            $job->onQueue($flowRun->queue);
        }

        if (config('saga-lara-flow.queue.after_commit')) {
            $job->afterCommit();
        }
    }

    private function resolveAction(): ?ActionRun
    {
        /** @var class-string<ActionRun> $model */
        $model = config('saga-lara-flow.models.action_run');

        return $model::query()->find($this->actionRunId);
    }
}
