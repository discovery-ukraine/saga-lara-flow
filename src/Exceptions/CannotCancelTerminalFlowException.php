<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

class CannotCancelTerminalFlowException extends FlowException
{
    public static function for(FlowRun $run): self
    {
        return new self("Flow run [{$run->id}] is already terminal ([{$run->status->value}]) and cannot be cancelled.");
    }
}
