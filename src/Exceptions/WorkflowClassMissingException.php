<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

class WorkflowClassMissingException extends FlowException
{
    public static function for(string $class): self
    {
        return new self("Workflow class [{$class}] does not exist.");
    }
}
