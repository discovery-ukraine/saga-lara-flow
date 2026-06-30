<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Completes one compensatable step then parks forever on a signal that never
 * arrives. Used to exercise run expiration with a non-empty compensation stack:
 * the monitor rolls 'a' back (undo:a) before landing the run in Expired.
 */
final class ExpiringSagaWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')
            ->compensateWith(UndoAction::class, 'a')
            ->run();

        $this->awaitSignal('never');
    }
}
