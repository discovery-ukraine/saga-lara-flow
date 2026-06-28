<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Requests a single action at sequence 0. Used to exercise history-contract
 * mismatches by pre-seeding a different operation at that sequence.
 */
final class OneActionWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'value')->run();
    }
}
