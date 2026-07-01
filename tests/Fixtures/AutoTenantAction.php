<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Action;
use DiscoveryUkraine\SagaLaraFlow\Attributes\Tenancy;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;

/**
 * Opts into auto-restore via #[Tenancy(auto: true)] even when the config default
 * is off, proving the per-class attribute overrides config. Records the ambient
 * host tenant so the test can confirm it was restored around handle().
 */
#[Tenancy(auto: true)]
final class AutoTenantAction extends Action
{
    /**
     * @return array{context: array<int|string, mixed>|null, ambient: ?string}
     */
    public function handle(): array
    {
        return [
            'context' => SagaFlow::tenancyContext(),
            'ambient' => TenantSpy::$current,
        ];
    }
}
