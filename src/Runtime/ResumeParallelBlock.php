<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use Illuminate\Bus\Batch;

/**
 * An invokable object, not a Closure, so the batch repository can serialize it.
 */
final readonly class ResumeParallelBlock
{
    public function __construct(
        public string $flowRunId,
    ) {}

    public function __invoke(Batch $batch): void
    {
        $flowRun = app(FlowRepository::class)->findOnWriter($this->flowRunId);

        if ($flowRun === null) {
            return;
        }

        app(FlowResumer::class)->resume($flowRun);
    }
}
