<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

/**
 * Stand-in for a host application's tenancy state. Tests wire its static
 * accessors as the tenancy.capture/restore hooks: $current is the "ambient"
 * tenant a worker is in, so a leaking bracket would show up as a stale value
 * between runs. Reset it at the start of every test — it is process-global.
 */
final class TenantSpy
{
    public static ?string $current = null;

    public static function reset(): void
    {
        self::$current = null;
    }

    /**
     * capture() hook: snapshot the ambient tenant.
     *
     * @return array{tenant: ?string}
     */
    public static function capture(): array
    {
        return ['tenant' => self::$current];
    }

    /**
     * restore() hook: enter the given tenant context (null = central).
     *
     * @param  array<int|string, mixed>  $context
     */
    public static function restore(array $context): void
    {
        self::$current = is_string($context['tenant'] ?? null) ? $context['tenant'] : null;
    }
}
