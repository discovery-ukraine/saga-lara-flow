<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

use DiscoveryUkraine\SagaLaraFlow\Models\CompensationRun;

/**
 * Surfaces a failed compensation as the secondary cause attached to a flow that
 * could not fully roll back (recorded under flow_run.exception['compensation']).
 */
class CompensationFailedException extends FlowException
{
    public static function for(CompensationRun $compensationRun): self
    {
        $label = $compensationRun->compensation_class ?? 'closure';

        return new self(sprintf(
            'Compensation %s at rollback sequence %d failed.',
            $label,
            $compensationRun->sequence,
        ));
    }
}
