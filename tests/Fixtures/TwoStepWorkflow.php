<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Runs two sequential actions. Used to prove that sync and queued execution
 * reach the same final database state.
 */
final class TwoStepWorkflow extends Workflow
{
    public function handle(string $orderId): void
    {
        $this->action(MakeValueAction::class, $orderId.'-1')->run();
        $this->action(MakeValueAction::class, $orderId.'-2')->run();
    }
}
