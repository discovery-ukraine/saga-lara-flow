<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * The failing step opts into being compensated on its own failure
 * (compensateStepOnSelfFailure), so the rollback runs undo:failed-step then undo:a.
 */
final class CompensateFailedStepWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')
            ->compensateWith(UndoAction::class, 'a')
            ->run();

        $this->action(ThrowingAction::class)
            ->compensateWith(UndoAction::class, 'failed-step')
            ->compensateStepOnSelfFailure()
            ->run();
    }
}
