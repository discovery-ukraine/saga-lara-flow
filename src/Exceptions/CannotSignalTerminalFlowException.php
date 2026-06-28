<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

class CannotSignalTerminalFlowException extends FlowException
{
    public static function for(FlowRun $flowRun): self
    {
        return new self(
            "Flow run [{$flowRun->id}] is already terminal ([{$flowRun->status->value}]) and cannot be signalled."
        );
    }
}
