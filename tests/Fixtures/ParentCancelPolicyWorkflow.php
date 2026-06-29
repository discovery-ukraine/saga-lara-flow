<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Parent with a compensatable step that awaits an active child under the Cancel
 * close policy. Used to exercise cancel() (no compensation) vs compensate() (with
 * compensation) on the waiting parent, cascading to the child.
 */
final class ParentCancelPolicyWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'parent')
            ->compensateWith(UndoAction::class, 'parent')
            ->run();

        $this->child(WaitingChildWithCompensationWorkflow::class)
            ->closePolicy(ChildClosePolicy::Cancel)
            ->run();
    }
}
