<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * A saga() group set to compensate in parallel, then a failing action. The two
 * group compensations form a single rollback level (run together in queued mode
 * via Bus::batch; sequentially in sync mode).
 */
final class ParallelSagaWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->saga()
            ->compensateInParallel()
            ->step(MakeValueAction::class, 'a')
            ->compensateWith(UndoAction::class, 'a')
            ->step(MakeValueAction::class, 'b')
            ->compensateWith(UndoAction::class, 'b')
            ->run();

        $this->action(ThrowingAction::class)->run();
    }
}
