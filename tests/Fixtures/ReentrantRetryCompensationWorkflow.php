<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Retry\RetryContext;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Throwable;

/**
 * The failing step opts its own side effect into the rollback
 * (compensateStepOnSelfFailure) and carries a predicate that reaches back into the
 * workflow DSL. The re-entry has to fail the run — but not at the cost of the
 * compensation the caller asked for on this very step.
 */
final class ReentrantRetryCompensationWorkflow extends Workflow
{
    /**
     * @throws Throwable
     */
    public function handle(string $orderId): void
    {
        $this->action(MakeValueAction::class, 'a')
            ->compensateWith(UndoAction::class, 'a')
            ->run();

        $this->action(DeclinableChargeAction::class, $orderId)
            ->compensateWith(UndoAction::class, 'charge')
            ->compensateStepOnSelfFailure()
            ->retryOnSignal(
                'balance-refilled',
                when: fn (RetryContext $context): bool => (bool) $this->awaitSignal('second-opinion'),
            )
            ->run();
    }
}
