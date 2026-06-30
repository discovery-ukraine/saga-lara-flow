<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Exceptions\AwaitSignalTimeoutException;
use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Awaits a signal with a past deadline and catches the resulting timeout, proving a
 * workflow can react to a signal timeout and carry on to completion instead of failing.
 */
final class SignalTimeoutCaughtWorkflow extends Workflow
{
    /**
     * @return array<string, mixed>
     */
    public function handle(): array
    {
        try {
            $payload = $this->awaitSignal('approval', timeout: now()->subMinute());

            return ['signal' => $payload];
        } catch (AwaitSignalTimeoutException) {
            return ['outcome' => 'timed-out'];
        }
    }
}
