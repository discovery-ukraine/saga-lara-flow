<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Awaits a child that parks, under the Cancel close policy, so closing the parent
 * cascades into a child close that has to plan the child's rollback first. The
 * child is the one whose replay can be made to throw.
 */
final class ParentOfVanishingChildWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'parent')
            ->compensateWith(UndoAction::class, 'parent')
            ->run();

        $this->child(VanishedArgumentWorkflow::class)
            ->closePolicy(ChildClosePolicy::Cancel)
            ->run();
    }
}
