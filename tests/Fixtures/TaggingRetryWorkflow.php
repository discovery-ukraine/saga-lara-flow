<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Retry\RetryContext;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Throwable;

/**
 * A retry predicate that tags the run it is deciding for. tag() is the one
 * workflow-facing write that never takes an ordinal, so the guard in
 * nextSequence() cannot see it.
 */
final class TaggingRetryWorkflow extends Workflow
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
                    $this->tag('decided', 'yes');

                    return true;
                },
            )
            ->run();
    }
}
