<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Action;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;

/**
 * Records, from inside handle(), both the run's tenant context (via the facade,
 * always available) and the ambient host tenant (only restored when auto is on).
 * Returned as the action result so a test can assert what the step actually saw.
 */
final class RecordTenantAction extends Action
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
