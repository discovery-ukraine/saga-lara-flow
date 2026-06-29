<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Child workflow that parks on a signal, so it stays active (Waiting) while a
 * parent decides what to do with it (close policy / external cancel).
 *
 * @return array{done: bool}
 */
final class WaitingChildWorkflow extends Workflow
{
    /**
     * @return array{done: bool}
     */
    public function handle(): array
    {
        $this->awaitSignal('child.go');

        return ['done' => true];
    }
}
