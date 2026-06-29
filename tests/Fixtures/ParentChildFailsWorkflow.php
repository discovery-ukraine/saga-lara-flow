<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Parent with its own compensatable step that awaits a failing child under the
 * default (propagate) policy: the child rolls itself back, the failure surfaces in
 * the parent, and the parent compensates too. Expected log: undo:child-a, undo:parent.
 */
final class ParentChildFailsWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'parent')
            ->compensateWith(UndoAction::class, 'parent')
            ->run();

        $this->child(FailingChildWorkflow::class)->run();
    }
}
