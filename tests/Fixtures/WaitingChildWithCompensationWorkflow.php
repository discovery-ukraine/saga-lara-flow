<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Child workflow that completes a compensatable step then parks on a signal, so a
 * parent-driven close with compensation rolls back undo:child.
 */
final class WaitingChildWithCompensationWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'child')
            ->compensateWith(UndoAction::class, 'child')
            ->run();

        $this->awaitSignal('child.go');
    }
}
