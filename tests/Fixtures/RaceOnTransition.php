<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use Closure;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\States\FlowStateMachine;

/**
 * Acts in the gap a caller leaves between planning a rollback and taking control of
 * the run: the callback runs once, immediately before the transition into the given
 * status is written, which is where a competing worker's owed attempt would land.
 */
final class RaceOnTransition extends FlowStateMachine
{
    /** @var ?Closure(FlowRun): void */
    public static ?Closure $race = null;

    public static FlowStatus $on = FlowStatus::Cancelling;

    public static int $races = 0;

    public static function reset(): void
    {
        self::$race = null;
        self::$on = FlowStatus::Cancelling;
        self::$races = 0;
    }

    public function transition(FlowRun $run, FlowStatus $to): FlowRun
    {
        if (self::$race !== null && $to === self::$on) {
            $race = self::$race;

            self::$race = null;
            self::$races++;

            $race($run);
        }

        return parent::transition($run, $to);
    }
}
