<?php

namespace DiscoveryUkraine\SagaLaraFlow\Contracts;

/**
 * Optional bridge to the host application's tenancy. Implementations capture the
 * current tenant context at creation time and restore it before each execution or
 * replay, so every job/worker runs in the correct tenant. The package binds to
 * callable hooks (config tenancy.capture/restore) rather than any specific
 * tenancy package.
 */
interface TenancyBridge
{
    /**
     * @return array<string, mixed>
     */
    public function capture(): array;

    /**
     * @param  array<string, mixed>  $context
     */
    public function restore(array $context): void;
}
