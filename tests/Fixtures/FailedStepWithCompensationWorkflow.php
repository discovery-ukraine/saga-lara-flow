<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * The failing step DEFINES a compensation but does NOT opt into compensate-on-failure.
 * With the default config the failed step is not compensated (only undo:a runs);
 * enabling sagas.compensate_failed_step in config makes it run undo:failed-step too.
 */
final class FailedStepWithCompensationWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')
            ->compensateWith(UndoAction::class, 'a')
            ->run();

        $this->action(ThrowingAction::class)
            ->compensateWith(UndoAction::class, 'failed-step')
            ->run();
    }
}
