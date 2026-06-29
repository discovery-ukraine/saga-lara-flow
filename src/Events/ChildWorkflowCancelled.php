<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

final readonly class ChildWorkflowCancelled
{
    public function __construct(
        public FlowRun $childFlowRun,
    ) {}
}
