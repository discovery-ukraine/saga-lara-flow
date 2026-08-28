<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Throwable;

/**
 * The three-step saga of RetryOnSignalWorkflow, with the middle step's policy
 * expressed as a RetryPolicy object instead of four arguments. The surrounding steps
 * are compensatable, so a test can prove that a refused failure rolls back exactly
 * what a policy-less failure would have.
 */
final class RetryPolicyWorkflow extends Workflow
{
    /**
     * @return array{created: mixed, charged: mixed, shipped: mixed}
     *
     * @throws Throwable
     */
    public function handle(string $orderId): array
    {
        $created = $this->action(MakeValueAction::class, 'created')
            ->compensateWith(UndoAction::class, 'created')
            ->run();

        $charged = $this->action(DeclinableChargeAction::class, $orderId)
            ->compensateWith(UndoAction::class, 'charged')
            ->retryOnSignal(new RecordingRetryPolicy)
            ->run();

        $shipped = $this->action(MakeValueAction::class, 'shipped')->run();

        return ['created' => $created, 'charged' => $charged, 'shipped' => $shipped];
    }
}
