<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * First action throws a business error; the second must never be scheduled.
 */
final class FailingWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(ThrowingAction::class)->run();
        $this->action(MakeValueAction::class, 'never')->run();
    }
}
