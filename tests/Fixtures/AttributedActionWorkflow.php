<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Runs a single attributed action, so the scheduled action_run carries the
 * #[ActionName] / #[ActionTimeout] declared on AttributedAction.
 */
final class AttributedActionWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(AttributedAction::class)->run();
    }
}
