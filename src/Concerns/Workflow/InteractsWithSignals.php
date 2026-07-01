<?php

namespace DiscoveryUkraine\SagaLaraFlow\Concerns\Workflow;

use DateTimeInterface;
use DiscoveryUkraine\SagaLaraFlow\Builders\SignalWaitBuilder;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\AwaitSignalTimeoutException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FlowSuspended;
use DiscoveryUkraine\SagaLaraFlow\Runtime\SignalWaiter;

/**
 * The awaitSignal()/signal() DSL: waiting for external signals.
 */
trait InteractsWithSignals
{
    /**
     * Wait for an external signal, identified by the operation's (flow_run_id,
     * sequence) ordinal. Returns the signal payload once delivered: if a matching
     * signal already arrived it resolves inline, otherwise the flow suspends and
     * resumes when the signal is delivered. Replays return the same payload.
     *
     * Passing $timeout persists a deadline on the wait-marker: once it passes the
     * monitor times the wait out and this throws AwaitSignalTimeoutException on the
     * next replay (catch it to react, or let it fail and roll back the flow).
     *
     * @throws AwaitSignalTimeoutException the wait passed its timeout deadline
     * @throws HistoryContractMismatchException handle() diverged from recorded history
     * @throws FlowSuspended internal control signal — never catch this in handle()
     */
    public function awaitSignal(string $name, ?DateTimeInterface $timeout = null): mixed
    {
        return app(SignalWaiter::class)->await($this->runtime, $name, $timeout);
    }

    /**
     * Fluent form of awaitSignal: $this->signal('name')->timeoutAfter($when)->wait().
     */
    public function signal(string $name): SignalWaitBuilder
    {
        return new SignalWaitBuilder($this->runtime, $name);
    }
}
