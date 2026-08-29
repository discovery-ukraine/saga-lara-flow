<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * One step, and a counter on the action that says whether its body ran. Used where a
 * test has to tell "the engine recorded the step" apart from "the engine executed it".
 */
final class CountedActionWorkflow extends Workflow
{
    /**
     * @return array{charged: string, calls: int}
     */
    public function handle(string $orderId = 'order-1'): array
    {
        return $this->action(FlakyPaymentAction::class, $orderId)->run();
    }
}
