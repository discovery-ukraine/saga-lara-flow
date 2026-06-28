<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Requests a single side effect at sequence 0. Used to exercise the history
 * contract when an action is pre-seeded at that sequence instead.
 */
final class SideEffectOnlyWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->sideEffect('token', fn () => 'value');
    }
}
