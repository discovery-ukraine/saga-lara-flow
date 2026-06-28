<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

final readonly class FlowCompleted
{
    public function __construct(
        public FlowRun $flowRun,
    ) {}
}
