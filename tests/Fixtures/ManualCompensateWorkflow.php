<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Completes a compensatable action, then parks on a signal. Used to exercise
 * manual compensate() on a non-terminal (Waiting) run: rolling back undo:a and
 * cancelling the run.
 */
final class ManualCompensateWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')
            ->compensateWith(UndoAction::class, 'a')
            ->run();

        $this->awaitSignal('go');

        $this->action(MakeValueAction::class, 'b')->run();
    }
}
