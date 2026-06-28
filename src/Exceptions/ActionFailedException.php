<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

/**
 * Business failure surfaced to the workflow when a previously executed action
 * step is found in the Failed state during replay. Catching this in handle()
 * is legitimate user error handling (it is not an internal control signal).
 */
class ActionFailedException extends FlowException
{
    public static function forAction(string $actionClass, int $sequence, string $message): self
    {
        return new self(sprintf(
            'Action %s at sequence %d failed: %s',
            $actionClass,
            $sequence,
            $message,
        ));
    }
}
