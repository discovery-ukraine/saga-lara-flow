<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * A WaitAllThenFail parallel block: two compensatable steps plus a failing one.
 * Every step settles first, then the flow fails and both completed steps roll
 * back together as one level (undo:a, undo:b).
 */
final class ParallelWaitAllWorkflow extends Workflow
{
    /**
     * @return list<mixed>
     */
    public function handle(): array
    {
        return $this->parallel()
            ->action(MakeValueAction::class, 'a')
            ->compensateWith(UndoAction::class, 'a')
            ->action(MakeValueAction::class, 'b')
            ->compensateWith(UndoAction::class, 'b')
            ->action(ThrowingAction::class)
            ->waitAllThenFail()
            ->run();
    }
}
