<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * A single required action carrying a wall-clock deadline already in the past. Used
 * to test the expiresAt() writer (the scheduled row persists expires_at) and the
 * monitor: the stuck step is expired, and on replay it fails the flow.
 */
final class ExpiringActionWorkflow extends Workflow
{
    /**
     * @return array{label: string}
     */
    public function handle(): array
    {
        return $this->action(MakeValueAction::class, 'a')
            ->expiresAt(now()->subMinute())
            ->run();
    }
}
