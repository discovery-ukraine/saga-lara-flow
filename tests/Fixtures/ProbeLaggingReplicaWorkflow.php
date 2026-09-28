<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Compensatable steps at sequences 0 and 2 around a child at 1 that fails and is carried
 * past with continueParentOnFailure(), then a signal wait at 3. A plan read from the
 * whole history holds 0 and 2; one that stops early holds 0 alone.
 */
final class ProbeLaggingReplicaWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')
            ->compensateWith(UndoAction::class, 'a')
            ->run();

        $this->child(FailingChildWorkflow::class)->continueParentOnFailure()->run();

        $this->action(MakeValueAction::class, 'b')
            ->compensateWith(UndoAction::class, 'b')
            ->run();

        $this->awaitSignal('go');
    }
}
