<?php

namespace DiscoveryUkraine\SagaLaraFlow\Jobs;

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Middleware\LockMiddlewareFactory;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use Throwable;

/**
 * Re-drives a suspended workflow after an awaited step (an action, and later a
 * signal/child) has progressed. Shares the run lock with RunWorkflowJob so a
 * resume is serialized against any other job for the same run.
 */
class ResumeWorkflowJob implements ShouldQueue
{
    use Queueable;

    public function __construct(
        public string $flowRunId,
    ) {}

    /**
     * @throws Throwable
     */
    public function handle(FlowExecutor $executor, FlowRepository $repository): void
    {
        $flowRun = $repository->find($this->flowRunId);

        if ($flowRun === null || $flowRun->isTerminal()) {
            return;
        }

        $executor->drive($flowRun, RunMode::Queued);
    }

    /**
     * @return array<int, object>
     */
    public function middleware(): array
    {
        return app(LockMiddlewareFactory::class)->workflowMiddleware($this->flowRunId);
    }
}
