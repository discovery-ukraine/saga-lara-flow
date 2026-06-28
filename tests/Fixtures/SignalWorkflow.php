<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Runs an action, waits for an external 'approval' signal at sequence 1, then
 * runs a second action whose label reflects the signal payload. Proves a flow
 * parks on awaitSignal and resumes with the delivered payload.
 */
final class SignalWorkflow extends Workflow
{
    public function handle(string $orderId): void
    {
        $this->action(MakeValueAction::class, $orderId.'-1')->run();

        $approval = $this->awaitSignal('approval');

        $this->action(MakeValueAction::class, 'approved-by-'.($approval['by'] ?? 'unknown'))->run();
    }
}
