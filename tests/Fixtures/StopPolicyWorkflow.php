<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Three compensatable actions then a failure. The middle compensation (undo b)
 * fails. With the default Stop policy the rollback halts there: undo c runs,
 * undo b fails, undo a is never reached. Rollback order is c, b, a.
 */
final class StopPolicyWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')
            ->compensateWith(UndoAction::class, 'a')
            ->run();

        $this->action(MakeValueAction::class, 'b')
            ->compensateWith(FailingUndoAction::class, 'b')
            ->run();

        $this->action(MakeValueAction::class, 'c')
            ->compensateWith(UndoAction::class, 'c')
            ->run();

        $this->action(ThrowingAction::class)->run();
    }
}
