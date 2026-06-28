<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Captures a side effect at sequence 0 (incrementing a shared counter), then
 * runs two sequential actions. The factory must run once no matter how many
 * times the workflow replays.
 */
final class SideEffectWorkflow extends Workflow
{
    public function handle(string $orderId): void
    {
        $token = $this->sideEffect('token', fn () => ++SideEffectCounter::$count);

        $this->action(MakeValueAction::class, $orderId.'-'.$token)->run();
        $this->action(MakeValueAction::class, $orderId.'-2')->run();
    }
}
