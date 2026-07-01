<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Runs a single AutoTenantAction (which carries #[Tenancy(auto: true)]) so a test
 * can prove the action-level attribute forces auto-restore under a config default
 * of off. The workflow itself has no #[Tenancy], so drive() does not auto-restore.
 */
final class AutoActionWorkflow extends Workflow
{
    /**
     * @return array{context: array<int|string, mixed>|null, ambient: ?string}
     */
    public function handle(): array
    {
        return $this->action(AutoTenantAction::class)->run();
    }
}
