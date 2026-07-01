<?php

namespace DiscoveryUkraine\SagaLaraFlow\Builders;

use DateTimeInterface;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FlowSuspended;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRuntime;
use DiscoveryUkraine\SagaLaraFlow\Runtime\SignalWaiter;

/**
 * Fluent form of awaitSignal: $this->signal('name')->timeoutAfter($when)->wait().
 * Equivalent to $this->awaitSignal('name', timeout: $when).
 *
 * timeoutAfter() persists a deadline on the wait-marker; the monitor times the wait
 * out after it passes and wait() then throws AwaitSignalTimeoutException on replay.
 */
final class SignalWaitBuilder
{
    private ?DateTimeInterface $timeout = null;

    public function __construct(
        private readonly FlowRuntime $runtime,
        private readonly string $name,
    ) {}

    /**
     * Set a wait deadline. The monitor flips the wait-marker to TimedOut once it
     * passes, and wait() surfaces AwaitSignalTimeoutException on the next replay.
     */
    public function timeoutAfter(?DateTimeInterface $timeout): SignalWaitBuilder
    {
        $this->timeout = $timeout;

        return $this;
    }

    /**
     * @throws FlowSuspended
     */
    public function wait(): mixed
    {
        return app(SignalWaiter::class)->await($this->runtime, $this->name, $this->timeout);
    }
}
