<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

/**
 * A hook class that exists but that the container cannot build.
 */
abstract class AbstractTenantHook
{
    /**
     * @return array<int|string, mixed>
     */
    abstract public function __invoke(): array;
}
