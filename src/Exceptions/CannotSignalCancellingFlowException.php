<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

class CannotSignalCancellingFlowException extends CannotSignalFlowException
{
    public static function for(FlowRun $flowRun): self
    {
        return new self(
            "Flow run [{$flowRun->id}] is rolling back ([{$flowRun->status->value}]) and cannot be signalled."
        );
    }
}
