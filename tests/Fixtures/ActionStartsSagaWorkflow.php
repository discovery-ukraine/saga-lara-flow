<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Throwable;

/**
 * The nesting that is meant to work: an ordinary step that happens to start a saga
 * of its own.
 */
final class ActionStartsSagaWorkflow extends Workflow
{
    /**
     * @throws Throwable
     */
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')
            ->compensateWith(UndoAction::class, 'a')
            ->run();

        $this->action(StartsASagaAction::class)
            ->compensateWith(UndoAction::class, 'inner')
            ->run();

        $this->action(ThrowingAction::class)->run();
    }
}
