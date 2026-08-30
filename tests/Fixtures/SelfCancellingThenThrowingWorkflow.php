<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use RuntimeException;

/**
 * Parks, and on a later replay moves its own run and then throws. It stands in for the
 * two things happening at once that no single-process test can otherwise stage: a
 * planning replay that faults while somebody else is moving the run under it — a
 * manual compensate() taking it to Cancelling, or a cancel() finishing it outright.
 */
final class SelfCancellingThenThrowingWorkflow extends Workflow
{
    public static ?FlowStatus $moveTo = null;

    public static function reset(): void
    {
        self::$moveTo = null;
    }

    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->run();

        if (self::$moveTo !== null) {
            self::$moveTo === FlowStatus::Cancelled
                ? SagaFlow::loadFlow($this->runId())->cancel()
                : app(StateMachine::class)->transition(SagaFlow::findRun($this->runId()), self::$moveTo);

            throw new RuntimeException('planning faulted while the run was being moved');
        }

        $this->awaitSignal('go');
    }
}
