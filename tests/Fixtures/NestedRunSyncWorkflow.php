<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Throwable;

/**
 * Drives a second run from inside handle(), where child() belongs. Every replay
 * starts another one, which is why it stays a mistake; what it must not do is
 * corrupt the pass that made the call.
 */
final class NestedRunSyncWorkflow extends Workflow
{
    /**
     * @throws Throwable
     */
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')
            ->compensateWith(UndoAction::class, 'a')
            ->run();

        SagaFlow::create(OneActionWorkflow::class)->runSync();

        $this->action(MakeValueAction::class, 'b')
            ->compensateWith(UndoAction::class, 'b')
            ->run();

        $this->action(ThrowingAction::class)->run();
    }
}
