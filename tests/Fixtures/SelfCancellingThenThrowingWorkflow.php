<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use RuntimeException;

/**
 * Parks, and on a later replay finishes its own run and then throws. It stands in for
 * the two things happening at once that no single-process test can otherwise stage: a
 * planning replay that faults while somebody else finalizes the run under it.
 */
final class SelfCancellingThenThrowingWorkflow extends Workflow
{
    public static bool $interfere = false;

    public static function reset(): void
    {
        self::$interfere = false;
    }

    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->run();

        if (self::$interfere) {
            SagaFlow::loadFlow($this->runId())->cancel();

            throw new RuntimeException('planning faulted while the run was being finished');
        }

        $this->awaitSignal('go');
    }
}
