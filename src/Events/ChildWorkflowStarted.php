<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

final readonly class ChildWorkflowStarted
{
    public function __construct(
        public FlowRun $parentFlowRun,
        public FlowRun $childFlowRun,
    ) {}
}
