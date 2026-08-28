<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Retry\RetryContext;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Throwable;

/**
 * A retry predicate that drives a second run to completion. The second run is not
 * the one being decided, so the id-specific guards let it through — but the
 * executor is a singleton and the nested pass rebinds and resets the very runtime
 * the outer pass is suspended inside.
 */
final class NestedDriveRetryWorkflow extends Workflow
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
                    SagaFlow::create(OneActionWorkflow::class)->runSync();

                    return true;
                },
            )
            ->run();
    }
}
