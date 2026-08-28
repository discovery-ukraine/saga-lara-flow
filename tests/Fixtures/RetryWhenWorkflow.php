<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Retry\RetryContext;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Throwable;

/**
 * The when: shorthand instead of a policy class. The predicate closes over a
 * workflow argument, which is the only state a replay reproduces — a closure that
 * captured anything else would answer differently on the next pass.
 */
final class RetryWhenWorkflow extends Workflow
{
    /**
     * @return array{charged: mixed, shipped: mixed}
     *
     * @throws Throwable
     */
    public function handle(string $orderId, ?int $refuseCode = null): array
    {
        $charged = $this->action(DeclinableChargeAction::class, $orderId)
            ->compensateWith(UndoAction::class, 'charged')
            ->retryOnSignal(
                'balance-refilled',
                when: fn (RetryContext $context): bool => $context->failure->code !== $refuseCode,
            )
            ->run();

        $shipped = $this->action(MakeValueAction::class, 'shipped')->run();

        return ['charged' => $charged, 'shipped' => $shipped];
    }
}
