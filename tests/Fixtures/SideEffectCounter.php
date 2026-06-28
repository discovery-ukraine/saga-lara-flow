<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

/**
 * Shared invocation counter used to prove a side-effect factory runs exactly
 * once across the many replays a single run performs. Reset it per test.
 */
final class SideEffectCounter
{
    public static int $count = 0;

    public static function reset(): void
    {
        self::$count = 0;
    }
}
