<?php

namespace DiscoveryUkraine\SagaLaraFlow\Jobs;

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Middleware\LockMiddlewareFactory;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;

/**
 * Entry point for a queued workflow run: drives the flow until it suspends,
 * completes or fails. Single-threaded per run via WithoutOverlapping.
 */
class RunWorkflowJob implements ShouldQueue
{
    use Queueable;

    public function __construct(
        public string $flowRunId,
    ) {}

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
