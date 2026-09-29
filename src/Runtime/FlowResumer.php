<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Jobs\ResumeWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

/**
 * Wakes a run once work it waits on has moved: a replay of the pass already driving it
 * in this process, or a ResumeWorkflowJob on the run's own connection and queue.
 */
final readonly class FlowResumer
{
    public function __construct(
        private FlowExecutor $executor,
    ) {}

    public function resume(FlowRun $flowRun): void
    {
        if ($this->executor->replayIfDriving($flowRun->id)) {
            return;
        }

        $job = ResumeWorkflowJob::dispatch($flowRun->id);

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
}
