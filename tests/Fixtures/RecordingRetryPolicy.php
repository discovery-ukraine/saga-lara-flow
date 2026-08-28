<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Retry\RetryContext;
use DiscoveryUkraine\SagaLaraFlow\Retry\RetryPolicy;
use RuntimeException;
use Throwable;

/**
 * A retry policy that records every context it was handed and answers according to
 * statics a test sets up front. The statics are how a test observes it at all: the
 * workflow constructs a fresh instance on every replay, so an instance property
 * would not survive to be asserted on — and in queued mode the cycles run in
 * separate jobs.
 */
final class RecordingRetryPolicy extends RetryPolicy
{
    /**
     * @var list<RetryContext>
     */
    public static array $seen = [];

    public static string $signalName = 'balance-refilled';

    /**
     * How often the builder read the policy's configuration, as against how often it
     * put a failure to the predicate. The two are deliberately different numbers.
     */
    public static int $configReads = 0;

    public static ?int $refuseCode = null;

    public static bool $throws = false;

    public static ?int $maxRetries = null;

    public static ?int $waitSeconds = null;

    /**
     * @var list<class-string<Throwable>>|null
     */
    public static ?array $only = null;

    public static function reset(): void
    {
        self::$seen = [];
        self::$configReads = 0;
        self::$signalName = 'balance-refilled';
        self::$refuseCode = null;
        self::$throws = false;
        self::$maxRetries = null;
        self::$waitSeconds = null;
        self::$only = null;
    }

    public static function calls(): int
    {
        return count(self::$seen);
    }

    public static function last(): ?RetryContext
    {
        return self::$seen === [] ? null : self::$seen[array_key_last(self::$seen)];
    }

    public function signal(): string
    {
        self::$configReads++;

        return self::$signalName;
    }

    public function maxRetries(): ?int
    {
        return self::$maxRetries;
    }

    public function waitSeconds(): ?int
    {
        return self::$waitSeconds;
    }

    /**
     * @return list<class-string<Throwable>>|null
     */
    public function only(): ?array
    {
        return self::$only;
    }

    public function shouldRetry(RetryContext $context): bool
    {
        self::$seen[] = $context;

        if (self::$throws) {
            throw new RuntimeException('the policy itself is broken');
        }

        return self::$refuseCode === null || $context->failure->code !== self::$refuseCode;
    }
}
