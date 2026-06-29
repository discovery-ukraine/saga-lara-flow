<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Action with a closure-based compensation, then a failing action. Proves
 * closure compensations are registered and executed on rollback.
 */
final class ClosureCompensationWorkflow extends Workflow
{
    public function handle(): void
    {
        $id = 'x';

        $this->action(MakeValueAction::class, $id)
            ->compensateWith(fn () => CompensationLog::record('closure-undo:'.$id))
            ->run();

        $this->action(ThrowingAction::class)->run();
    }
}
