<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FlowSuspended;

/**
 * The single place that raises the internal suspension control signal. Runtime
 * seams (ActionBuilder, SignalWaiter, …) call this instead of throwing
 * FlowSuspended directly.
 */
final class FlowSuspender
{
    /**
     * Suspend the flow until an awaited step progresses (queued dispatch done, or
     * a genuine external wait).
     *
     * @throws FlowSuspended
     */
    public function suspend(string $reason, int $sequence): never
    {
        throw new FlowSuspended($reason, $sequence);
    }

    /**
     * Sync mode: the step ran inline; signal the drive loop to replay from the top.
     *
     * @throws FlowSuspended
     */
    public function suspendInline(string $reason, int $sequence): never
    {
        throw new FlowSuspended($reason, $sequence, inlineResolved: true);
    }
}
