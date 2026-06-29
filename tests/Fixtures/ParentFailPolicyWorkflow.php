<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Parent awaiting an active child under the Fail close policy: closing the parent
 * forces the child to Failed (with its own compensation rolled back).
 */
final class ParentFailPolicyWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->child(WaitingChildWithCompensationWorkflow::class)
            ->closePolicy(ChildClosePolicy::Fail)
            ->run();
    }
}
