<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Exceptions\AwaitSignalTimeoutException;
use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Catches a signal timeout and carries on into a parallel block. The catch is what
 * makes the block reachable by a collecting replay: the timeout is surfaced during
 * collection precisely so a workflow that handles it still registers its later
 * compensations, which walks the replay into a block nothing has scheduled yet.
 */
final class CaughtTimeoutThenParallelWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')
            ->compensateWith(UndoAction::class, 'a')
            ->run();

        try {
            $this->awaitSignal('approval', timeout: now()->subMinute());
        } catch (AwaitSignalTimeoutException) {
            // The block below is what the run does instead.
        }

        $this->parallel()
            ->action(MakeValueAction::class, 'p1')
            ->action(MakeValueAction::class, 'p2')
            ->run();
    }
}
