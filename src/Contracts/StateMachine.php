<?php

namespace DiscoveryUkraine\SagaLaraFlow\Contracts;

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\InvalidTransitionException;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

interface StateMachine
{
    /**
     * Transition a flow run to the given status, persisting the change.
     *
     * @throws InvalidTransitionException
     */
    public function transition(FlowRun $run, FlowStatus $to): FlowRun;

    public function canTransition(FlowStatus $from, FlowStatus $to): bool;
}
