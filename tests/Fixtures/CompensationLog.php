<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

/**
 * Test-only sink that records the order in which compensations actually executed,
 * so a test can assert reverse (LIFO) rollback order and that closure/class
 * compensations really ran. Reset in beforeEach.
 */
final class CompensationLog
{
    /**
     * @var list<string>
     */
    public static array $entries = [];

    public static function record(string $label): void
    {
        self::$entries[] = $label;
    }

    public static function reset(): void
    {
        self::$entries = [];
    }

    /**
     * @return list<string>
     */
    public static function all(): array
    {
        return self::$entries;
    }
}
