<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

class FlowNotFoundException extends FlowException
{
    public static function for(string $id): self
    {
        return new self("Flow run [{$id}] was not found.");
    }
}
