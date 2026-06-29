<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Two compensatable actions then a failing one. On failure the completed steps
 * roll back in reverse order: undo:b then undo:a.
 */
final class SagaRollbackWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')
            ->compensateWith(UndoAction::class, 'a')
            ->run();

        $this->action(MakeValueAction::class, 'b')
            ->compensateWith(UndoAction::class, 'b')
            ->run();

        $this->action(ThrowingAction::class)->run();
    }
}
