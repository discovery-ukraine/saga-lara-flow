<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Retry\RetryContext;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Throwable;

/**
 * A retry predicate that rolls back a different run. compensate() reconstructs the
 * stack by a compensation-only replay, which drives the shared runtime just as a
 * nested run does — the target being somebody else's run does not make it safe.
 */
final class NestedCompensateRetryWorkflow extends Workflow
{
    /**
     * @throws Throwable
     */
    public function handle(string $orderId, string $otherRunId): mixed
    {
        return $this->action(DeclinableChargeAction::class, $orderId)
            ->retryOnSignal(
                'balance-refilled',
                when: function (RetryContext $context) use ($otherRunId): bool {
                    SagaFlow::loadFlow($otherRunId)->compensate();

                    return true;
                },
            )
            ->run();
    }
}
