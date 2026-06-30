<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

/**
 * The doctor re-woke a flow: a stuck Waiting run whose resume was lost
 * (reason "lost_resume"), or a manual saga-flow:kick (reason "manual").
 */
final readonly class FlowRewoken
{
    public function __construct(
        public FlowRun $flowRun,
        public string $reason,
    ) {}
}
