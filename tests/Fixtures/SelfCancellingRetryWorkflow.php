<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Retry\RetryContext;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Throwable;

/**
 * A retry predicate that cancels the very run it is deciding for. Left to run, the
 * cancellation would settle the run's open rows and the seam would then park the
 * step anyway, leaving a live wait under a terminal run.
 */
final class SelfCancellingRetryWorkflow extends Workflow
{
    /**
     * @throws Throwable
     */
    public function handle(string $orderId): mixed
    {
        return $this->action(DeclinableChargeAction::class, $orderId)
            ->retryOnSignal(
                'balance-refilled',
                when: function (RetryContext $context): bool {
                    SagaFlow::loadFlow($context->runId)->cancel('decided against it');

                    return true;
                },
            )
            ->run();
    }
}
