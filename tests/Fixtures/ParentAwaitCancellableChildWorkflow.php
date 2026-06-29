<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Parent that simply awaits a child. Used to exercise an external cancellation of
 * the child while the parent waits: the wait then resolves as a business error.
 */
final class ParentAwaitCancellableChildWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->child(WaitingChildWorkflow::class)->run();
    }
}
