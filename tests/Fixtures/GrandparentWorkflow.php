<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Top of a three-level tree (grandparent → parent → grandchild), each awaiting the
 * next under the Cancel close policy, so cancelling the grandparent cascades down.
 */
final class GrandparentWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->child(ParentOfGrandchildWorkflow::class)
            ->closePolicy(ChildClosePolicy::Cancel)
            ->run();
    }
}
