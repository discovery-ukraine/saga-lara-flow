<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Action;
use RuntimeException;

/**
 * A payment step whose failure carries a controllable exception code, so a test can
 * drive a retry policy that branches on something other than the exception class.
 * The counters are static (like FlakyPaymentAction) because the cycles happen across
 * separate replays — and, in queued mode, separate jobs — of the same run.
 */
final class DeclinableChargeAction extends Action
{
    public static int $failures = 0;

    public static int $calls = 0;

    public static int $code = 0;

    public static function reset(int $failures = 0, int $code = 0): void
    {
        self::$failures = $failures;
        self::$calls = 0;
        self::$code = $code;
    }

    /**
     * @return array{charged: string, calls: int}
     */
    public function handle(string $orderId): array
    {
        self::$calls++;

        if (self::$calls <= self::$failures) {
            throw new RuntimeException('charge declined', self::$code);
        }

        return ['charged' => $orderId, 'calls' => self::$calls];
    }
}
