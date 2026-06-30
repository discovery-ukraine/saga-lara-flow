<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * One compensatable step followed by a required step with a past deadline. When the
 * monitor expires the second step, replay fails the flow and rolls the first back
 * (undo:a), proving an expired action triggers compensation.
 */
final class ExpiringActionSagaWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')
            ->compensateWith(UndoAction::class, 'a')
            ->run();

        $this->action(MakeValueAction::class, 'b')
            ->expiresAt(now()->subMinute())
            ->run();
    }
}
