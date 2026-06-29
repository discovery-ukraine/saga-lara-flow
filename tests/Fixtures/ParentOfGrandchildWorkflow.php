<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Middle layer of a three-level tree: awaits a grandchild under the Cancel close
 * policy, so cancelling the grandparent cascades down to the grandchild.
 */
final class ParentOfGrandchildWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->child(WaitingChildWorkflow::class)
            ->closePolicy(ChildClosePolicy::Cancel)
            ->run();
    }
}
