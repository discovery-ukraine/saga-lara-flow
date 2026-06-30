<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * A FailFast parallel block: one compensatable step plus a failing one. The hard
 * failure fails the flow; the completed step rolls back (undo:a).
 */
final class ParallelFailFastWorkflow extends Workflow
{
    /**
     * @return list<mixed>
     */
    public function handle(): array
    {
        return $this->parallel()
            ->action(MakeValueAction::class, 'a')
            ->compensateWith(UndoAction::class, 'a')
            ->action(ThrowingAction::class)
            ->failFast()
            ->run();
    }
}
