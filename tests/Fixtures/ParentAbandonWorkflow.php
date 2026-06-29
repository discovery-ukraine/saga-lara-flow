<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Parent awaiting an active child under the Abandon close policy: closing the
 * parent leaves the child running.
 */
final class ParentAbandonWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->child(WaitingChildWorkflow::class)
            ->closePolicy(ChildClosePolicy::Abandon)
            ->run();
    }
}
