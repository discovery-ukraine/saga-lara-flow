<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

final readonly class ChildWorkflowExpired
{
    public function __construct(
        public FlowRun $childFlowRun,
    ) {}
}
