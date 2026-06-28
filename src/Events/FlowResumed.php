<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

final readonly class FlowResumed
{
    public function __construct(
        public FlowRun $flowRun,
    ) {}
}
