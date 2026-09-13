<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

/**
 * An invokable tenancy.capture hook, named by class so config:cache can store it.
 */
final class CaptureTenant
{
    /**
     * @return array{tenant: ?string}
     */
    public function __invoke(): array
    {
        return TenantSpy::capture();
    }
}
