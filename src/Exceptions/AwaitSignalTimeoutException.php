<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;

/**
 * Business failure surfaced to the workflow when an awaited signal is found in the
 * TimedOut state during replay — the monitor flipped its wait-marker after its
 * timeout_at deadline passed (§15). Like ActionFailedException it is a legitimate
 * business error (not an internal control signal): catching it in handle() lets the
 * workflow react to the timeout; leaving it uncaught fails the flow and rolls back.
 */
class AwaitSignalTimeoutException extends FlowException
{
    public static function for(FlowSignal $signal, int $sequence): self
    {
        return new self(sprintf(
            "Signal '%s' timed out at sequence %d.",
            $signal->name,
            $sequence,
        ));
    }
}
