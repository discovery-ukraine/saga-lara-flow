<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Retry\RetryContext;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Throwable;

/**
 * A retry predicate that reaches back into the workflow DSL instead of answering.
 * The step after it is what makes the attempt harmful: the predicate is not asked
 * again once the guarded step succeeds, so an ordinal it consumed would be left
 * unclaimed and this step would land in the wrong slot.
 */
final class SuspendingRetryWorkflow extends Workflow
{
    /**
     * @return array{charged: mixed, shipped: mixed}
     *
     * @throws Throwable
     */
    public function handle(string $orderId): array
    {
        $charged = $this->action(DeclinableChargeAction::class, $orderId)
            ->retryOnSignal(
                'balance-refilled',
                when: fn (RetryContext $context): bool => (bool) $this->awaitSignal('second-opinion'),
            )
            ->run();

        $shipped = $this->action(MakeValueAction::class, 'shipped')->run();

        return ['charged' => $charged, 'shipped' => $shipped];
    }
}
