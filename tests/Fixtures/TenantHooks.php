<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

/**
 * Tenancy hooks as instance methods, named as a [class, method] pair: not callable
 * as written, so the pair has to be resolved from the container first.
 */
final class TenantHooks
{
    /**
     * @param  array<int|string, mixed>  $context
     */
    public function restore(array $context): void
    {
        TenantSpy::restore($context);
    }
}
