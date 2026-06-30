<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * A successful parallel block of two compensatable steps, then a failing action.
 * The later failure rolls the whole block back as ONE parallel level (both undos
 * share the block's parallel group id), so undo:a and undo:b run together.
 */
final class ParallelCompensationWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->parallel()
            ->action(MakeValueAction::class, 'a')
            ->compensateWith(UndoAction::class, 'a')
            ->action(MakeValueAction::class, 'b')
            ->compensateWith(UndoAction::class, 'b')
            ->run();

        $this->action(ThrowingAction::class)->run();
    }
}
