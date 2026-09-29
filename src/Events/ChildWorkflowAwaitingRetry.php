<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Contracts\Events\ShouldDispatchAfterCommit;

final readonly class ChildWorkflowAwaitingRetry implements ShouldDispatchAfterCommit
{
    public function __construct(
        public FlowRun $childFlowRun,
        public string $signal,
    ) {}
}
