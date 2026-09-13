<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use RuntimeException;

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

    /**
     * A restore that enters the tenant and then fails, as one whose database is
     * unreachable part of the way through would.
     *
     * @param  array<int|string, mixed>  $context
     */
    public function restoreThenFail(array $context): void
    {
        TenantSpy::restore($context);

        throw new RuntimeException('tenant database unavailable');
    }

    /**
     * @param  array<int|string, mixed>|null  $previous
     */
    public function end(?array $previous): void
    {
        TenantSpy::restore($previous ?? []);
    }
}
