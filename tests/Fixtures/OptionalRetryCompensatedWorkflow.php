<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Throwable;

/**
 * OptionalRetryOnSignalWorkflow with a compensation on the optional step itself.
 * The compensation is what makes a collecting replay observable: it lands in the
 * stack only if the replay resolved the give-up rather than stopping at it.
 */
final class OptionalRetryCompensatedWorkflow extends Workflow
{
    /**
     * @throws Throwable
     */
    public function handle(string $orderId, ?int $maxRetries = null): mixed
    {
        return $this->action(FlakyPaymentAction::class, $orderId)
            ->continueOnFailure()
            ->fallbackValueOnFail('unpaid')
            ->compensateWith(UndoAction::class, 'charge')
            ->compensateStepOnSelfFailure()
            ->retryOnSignal('balance-refilled', maxRetries: $maxRetries)
            ->run();
    }
}
