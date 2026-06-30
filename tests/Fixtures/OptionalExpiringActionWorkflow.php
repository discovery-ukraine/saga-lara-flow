<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * An optional action with a past deadline. When the monitor expires it, replay
 * resolves the fallback (the expiry is tolerated), and the flow completes — an
 * expired optional step behaves like any other optional give-up.
 */
final class OptionalExpiringActionWorkflow extends Workflow
{
    public function handle(): mixed
    {
        return $this->action(MakeValueAction::class, 'a')
            ->continueOnFailure()
            ->fallbackValueOnFail(['skipped' => true])
            ->expiresAt(now()->subMinute())
            ->run();
    }
}
