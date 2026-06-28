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
 * timeoutAfter() is accepted for API stability (§4) but is a no-op until the
 * monitor lands in Phase 8; it neither persists timeout_at nor expires the wait.
 */
final readonly class SignalWaitBuilder
{
    public function __construct(
        private FlowRuntime $runtime,
        private string $name,
    ) {}

    /**
     * Set a wait deadline. No-op placeholder until Phase 8 (monitor): the value is
     * accepted for API stability (§4) but not yet persisted or enforced.
     */
    public function timeoutAfter(?DateTimeInterface $timeout): SignalWaitBuilder
    {
        return $this;
    }

    /**
     * @throws FlowSuspended
     */
    public function wait(): mixed
    {
        return app(SignalWaiter::class)->await($this->runtime, $this->name);
    }
}
