<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Throwable;

final readonly class FlowFailed
{
    public function __construct(
        public FlowRun $flowRun,
        public Throwable $exception,
    ) {}
}
