<?php

namespace DiscoveryUkraine\SagaLaraFlow\Jobs;

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Middleware\LockMiddlewareFactory;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionDispatcher;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionRecorder;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowResumer;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Database\Eloquent\Relations\Relation;
use Illuminate\Foundation\Queue\Queueable;
use Throwable;

/**
 * $retryGeneration is a declared, not promoted, property: an older payload is unserialized without
 * the constructor, and only a class-level default keeps it initialized (read as cycle 0).
 */
class RunActionJob implements ShouldQueue
{
    use Queueable;

    public int $tries;

    public int $timeout;

    public ?int $retryGeneration = null;

    public function __construct(
        public string $actionRunId,
        public string $actionClass,
        ?int $retryGeneration = null,
    ) {
        $this->retryGeneration = $retryGeneration;

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

        // Resumed either way: this job may be the only wake left if whatever rewound the row died.
        $dispatcher->execute($action, $this->retryGeneration ?? 0);

        $this->resumeWorkflow($action);
    }

    public function failed(Throwable $exception): void
    {
        $action = $this->resolveAction();

        if ($action === null) {
            return;
        }

        if ($action->retry_signal_attempts !== ($this->retryGeneration ?? 0)) {
            $this->resumeWorkflow($action);

            return;
        }

        app(ActionRecorder::class)->markQueueAttemptsExhausted($action);

        // A step with a retry policy is left to the seam: only it knows the only-filter and the
        // budget.
        if ($action->retry_signal === null
            && $action->continue_on_failure
            && $action->status === ActionStatus::Failed) {
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
        app(FlowResumer::class)->resume($action->flowRun);
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
