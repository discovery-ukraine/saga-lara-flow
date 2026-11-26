<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

/**
 * The doctor re-woke a flow: a Waiting run whose resume was lost ("lost_resume"), a Pending run
 * whose first job was lost ("lost_start"), or a manual saga-flow:kick ("manual").
 */
final readonly class FlowRewoken
{
    public function __construct(
        public FlowRun $flowRun,
        public string $reason,
    ) {}
}
