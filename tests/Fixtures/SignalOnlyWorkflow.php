<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Awaits a single 'go' signal at sequence 0. Used to exercise signal history
 * contracts by pre-seeding a different name or operation at that sequence.
 */
final class SignalOnlyWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->awaitSignal('go');
    }
}
