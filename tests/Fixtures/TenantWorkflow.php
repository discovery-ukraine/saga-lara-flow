<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Runs a single RecordTenantAction so a test can inspect the tenant context and
 * ambient host tenant observed inside a queued step.
 */
final class TenantWorkflow extends Workflow
{
    /**
     * @return array{context: array<int|string, mixed>|null, ambient: ?string}
     */
    public function handle(): array
    {
        return $this->action(RecordTenantAction::class)->run();
    }
}
