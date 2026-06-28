<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Waits for a 'go' signal at sequence 0, then runs an action using its payload.
 * Used to prove a signal delivered before the workflow awaits it is consumed
 * inline without suspending.
 */
final class EarlySignalWorkflow extends Workflow
{
    public function handle(): void
    {
        $signal = $this->awaitSignal('go');

        $this->action(MakeValueAction::class, 'got-'.($signal['v'] ?? 'none'))->run();
    }
}
