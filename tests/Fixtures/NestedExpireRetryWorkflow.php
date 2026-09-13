<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Retry\RetryContext;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Throwable;

/**
 * A retry predicate that expires another run, or replans its rollback: the two
 * public executor entries that plan by replaying and wrap what the replay throws.
 */
final class NestedExpireRetryWorkflow extends Workflow
{
    /**
     * @throws Throwable
     */
    public function handle(string $orderId, string $otherRunId, bool $replan = false): mixed
    {
        return $this->action(DeclinableChargeAction::class, $orderId)
            ->retryOnSignal('balance-refilled', when: function (RetryContext $context) use ($otherRunId, $replan): bool {
                $other = SagaFlow::findRun($otherRunId);

                $replan
                    ? app(FlowExecutor::class)->replanCompensations($other, [])
                    : app(FlowExecutor::class)->expireRun($other);

                return true;
            })
            ->run();
    }
}
