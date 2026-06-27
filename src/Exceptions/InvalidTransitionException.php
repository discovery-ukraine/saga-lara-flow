<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;

class InvalidTransitionException extends FlowException
{
    public static function between(FlowStatus $from, FlowStatus $to): self
    {
        return new self("Cannot transition flow run from [{$from->value}] to [{$to->value}].");
    }
}
