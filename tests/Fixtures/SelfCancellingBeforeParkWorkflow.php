<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Throwable;

/**
 * Cancels its own run on the replay that is about to park its failed step, staging in
 * one process what would otherwise need two: a cancellation landing after the failure
 * was recorded and before the parking commits.
 *
 * A queued cancellation arriving BEFORE the replay cannot reach the park at all — the
 * drive loop transitions to Running first, and no terminal status allows that — so the
 * window this fixture opens is the only one the queued path actually has.
 *
 * It times itself off the step's own row rather than a call counter, so it fires on the
 * parking pass whatever order the queue delivers in.
 */
final class SelfCancellingBeforeParkWorkflow extends Workflow
{
    public static bool $armed = false;

    public static function reset(): void
    {
        self::$armed = false;
    }

    /**
     * @return array{created: mixed, charged: mixed}
     *
     * @throws Throwable
     */
    public function handle(string $orderId): array
    {
        $created = $this->action(MakeValueAction::class, 'created')->run();

        if (self::$armed && $this->stepHasFailed()) {
            SagaFlow::findRun($this->runId())->markCancelled();
        }

        $charged = $this->action(FlakyPaymentAction::class, $orderId)
            ->retryOnSignal('balance-refilled')
            ->run();

        return ['created' => $created, 'charged' => $charged];
    }

    private function stepHasFailed(): bool
    {
        return ActionRun::query()
            ->where('flow_run_id', $this->runId())
            ->where('sequence', 1)
            ->where('status', ActionStatus::Failed)
            ->exists();
    }
}
