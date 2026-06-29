<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Child workflow that completes a compensatable step then fails, so it rolls
 * itself back (undo:child-a) and lands Failed — exercising how a failing child
 * propagates to (or is isolated from) its parent.
 */
final class FailingChildWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'child-a')
            ->compensateWith(UndoAction::class, 'child-a')
            ->run();

        $this->action(ThrowingAction::class)->run();
    }
}
