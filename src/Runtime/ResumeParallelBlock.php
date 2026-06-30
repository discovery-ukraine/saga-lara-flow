<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Jobs\ResumeWorkflowJob;
use Illuminate\Bus\Batch;

/**
 * The queued parallel-join continuation, run as a Bus::batch finally-callback once
 * every step of a parallel() block has reached a terminal state (or the block was
 * cancelled by FailFast). It is a plain invokable object (not a Closure) so the
 * batch repository can serialize it; it dispatches EXACTLY ONE ResumeWorkflowJob so
 * the drive loop replays once and the join resolves (returns results or fails).
 */
final readonly class ResumeParallelBlock
{
    public function __construct(
        public string $flowRunId,
    ) {}

    public function __invoke(Batch $batch): void
    {
        $flowRun = app(FlowRepository::class)->find($this->flowRunId);

        if ($flowRun === null) {
            return;
        }

        $job = ResumeWorkflowJob::dispatch($this->flowRunId);

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
