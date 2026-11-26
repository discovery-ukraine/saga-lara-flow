<?php

namespace DiscoveryUkraine\SagaLaraFlow\Jobs;

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Middleware\LockMiddlewareFactory;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use Throwable;

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
        $flowRun = $repository->findOnWriter($this->flowRunId);

        if ($flowRun === null || ! $flowRun->status->canStartWork()) {
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
