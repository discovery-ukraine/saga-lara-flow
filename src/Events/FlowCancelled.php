<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

final readonly class FlowCancelled
{
    public function __construct(
        public FlowRun $flowRun,
        public ?string $reason = null,
    ) {}
}
