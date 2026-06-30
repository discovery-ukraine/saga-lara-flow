<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Completes one compensatable step then awaits a signal with a deadline already in
 * the past. The monitor times the wait out; on replay awaitSignal throws
 * AwaitSignalTimeoutException, which (uncaught) fails the flow and rolls back 'a'.
 */
final class SignalTimeoutWorkflow extends Workflow
{
    public function handle(): mixed
    {
        $this->action(MakeValueAction::class, 'a')
            ->compensateWith(UndoAction::class, 'a')
            ->run();

        return $this->awaitSignal('approval', timeout: now()->subMinute());
    }
}
