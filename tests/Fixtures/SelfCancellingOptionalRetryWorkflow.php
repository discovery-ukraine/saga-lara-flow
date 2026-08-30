<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Throwable;

/**
 * The shape that proves a lost parking must stop the pass rather than resolve it: an
 * OPTIONAL retrying step with another step behind it. Resolving the give-up would hand
 * back the fallback and let handle() carry on into that second step, scheduling a row
 * and appending action.scheduled under a run whose settlement has already run once and
 * will never run again.
 *
 * It cancels its own run on the replay that is about to park, timing itself off the
 * step's row so it fires on that pass whatever order the queue delivers in.
 */
final class SelfCancellingOptionalRetryWorkflow extends Workflow
{
    public static bool $armed = false;

    public static function reset(): void
    {
        self::$armed = false;
    }

    /**
     * @return array{charged: mixed, shipped: mixed}
     *
     * @throws Throwable
     */
    public function handle(string $orderId): array
    {
        if (self::$armed && $this->stepHasFailed()) {
            SagaFlow::findRun($this->runId())->markCancelled();
        }

        $charged = $this->action(FlakyPaymentAction::class, $orderId)
            ->continueOnFailure()
            ->fallbackValueOnFail('unpaid')
            ->retryOnSignal('balance-refilled')
            ->run();

        $shipped = $this->action(MakeValueAction::class, 'shipped')->run();

        return ['charged' => $charged, 'shipped' => $shipped];
    }

    private function stepHasFailed(): bool
    {
        return ActionRun::query()
            ->where('flow_run_id', $this->runId())
            ->where('sequence', 0)
            ->where('status', ActionStatus::Failed)
            ->exists();
    }
}
