<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Throwable;

/**
 * The same policy object on a step inside a saga() group, so a test can prove the
 * group builder hands it through unchanged rather than dropping the predicate on
 * the way to the action builder.
 */
final class RetryPolicySagaWorkflow extends Workflow
{
    /**
     * @return list<mixed>
     *
     * @throws Throwable
     */
    public function handle(string $orderId): array
    {
        return $this->saga()
            ->step(MakeValueAction::class, 'created')
            ->compensateWith(UndoAction::class, 'created')
            ->step(DeclinableChargeAction::class, $orderId)
            ->compensateWith(UndoAction::class, 'charged')
            ->retryOnSignal(new RecordingRetryPolicy)
            ->step(MakeValueAction::class, 'shipped')
            ->run();
    }
}
