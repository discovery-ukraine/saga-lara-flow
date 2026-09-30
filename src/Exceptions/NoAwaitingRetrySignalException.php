<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

class NoAwaitingRetrySignalException extends CannotSignalFlowException
{
    public static function for(FlowRun $flowRun): self
    {
        return new self(
            "Flow run [{$flowRun->id}] has no step or child parked on a retry that a signal can still end."
        );
    }
}
