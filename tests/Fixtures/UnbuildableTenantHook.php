<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use RuntimeException;

/**
 * A hook class whose constructor throws, as one whose dependency is unavailable
 * would. The container lets that exception through as it is.
 */
final class UnbuildableTenantHook
{
    public function __construct()
    {
        throw new RuntimeException('hook dependency unavailable');
    }

    /**
     * @return array<int|string, mixed>
     */
    public function __invoke(): array
    {
        return [];
    }
}
