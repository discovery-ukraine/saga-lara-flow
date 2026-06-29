<?php

namespace DiscoveryUkraine\SagaLaraFlow\States;

use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\InvalidTransitionException;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

class FlowStateMachine implements StateMachine
{
    public function transition(FlowRun $run, FlowStatus $to): FlowRun
    {
        $from = $run->status;

        // Same-state transitions are idempotent no-ops (safe to call on replay).
        if ($from === $to) {
            return $run;
        }

        if (! $this->canTransition($from, $to)) {
            throw InvalidTransitionException::between($from, $to);
        }

        $now = now();

        $run->status = $to;

        if ($to === FlowStatus::Running && $run->started_at === null) {
            $run->started_at = $now;
        }

        if ($to === FlowStatus::Cancelled) {
            $run->cancelled_at = $now;
        }

        if ($to->isTerminal()) {
            $run->finished_at = $now;
        }

        $run->save();

        return $run;
    }

    public function canTransition(FlowStatus $from, FlowStatus $to): bool
    {
        return in_array($to, $this->allowedFrom($from), true);
    }

    /**
     * @return list<FlowStatus>
     */
    private function allowedFrom(FlowStatus $from): array
    {
        return match ($from) {
            FlowStatus::Pending => [
                FlowStatus::Running,
                FlowStatus::Cancelling,
                FlowStatus::Cancelled,
                FlowStatus::Expired,
            ],
            FlowStatus::Running => [
                FlowStatus::Waiting,
                FlowStatus::Completed,
                FlowStatus::Failed,
                FlowStatus::Cancelling,
                FlowStatus::Cancelled,
                FlowStatus::Expired,
            ],
            FlowStatus::Waiting => [
                FlowStatus::Running,
                FlowStatus::Completed,
                FlowStatus::Failed,
                FlowStatus::Cancelling,
                FlowStatus::Cancelled,
                FlowStatus::Expired,
            ],
            FlowStatus::Cancelling => [
                FlowStatus::Cancelled,
                FlowStatus::Failed,
            ],
            FlowStatus::Completed,
            FlowStatus::Failed,
            FlowStatus::Cancelled,
            FlowStatus::Expired => [],
        };
    }
}
