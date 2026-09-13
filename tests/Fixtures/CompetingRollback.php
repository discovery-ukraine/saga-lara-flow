<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;

/**
 * Commits, from inside a pass that is still replaying, what a competing process
 * commits first when it rolls the same run back: the move into Cancelling, made
 * through the engine's own state machine on a row this pass does not hold. One
 * process reaches the state two would, and the unwind that follows plays no part in
 * what the seams are asked afterwards.
 */
final class CompetingRollback
{
    public static function commit(string $flowRunId): void
    {
        $run = app(FlowRepository::class)->findOrFail($flowRunId);

        app(StateMachine::class)->transition($run, FlowStatus::Cancelling);
    }
}
