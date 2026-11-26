<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

use DiscoveryUkraine\SagaLaraFlow\Models\CompensationRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

/**
 * A compensation not settled when its level finished, recorded under exception['compensation'].
 * It may still finish later: a duplicate delivery can close a batch before the live worker records.
 */
class CompensationUnfinishedException extends FlowException
{
    public static function for(CompensationRun $compensationRun): self
    {
        $label = $compensationRun->compensation_class ?? 'closure';

        return new self(sprintf(
            'Compensation %s at rollback sequence %d had not finished when its rollback level ended (status: %s).',
            $label,
            $compensationRun->sequence,
            $compensationRun->status->value,
        ));
    }

    /**
     * A level not registered yet when the rollback was cancelled never ran.
     */
    public static function cutShort(FlowRun $flowRun): self
    {
        return new self(sprintf(
            'Rollback of flow %s [%s] was cancelled before it finished; every compensation registered by then had completed, and any level not yet registered never ran.',
            $flowRun->workflow_class,
            $flowRun->id,
        ));
    }
}
