<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Exceptions\MissingFlowContextException;
use DiscoveryUkraine\SagaLaraFlow\Retry\RetryContext;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRuntime;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Throwable;

/**
 * Reports, from inside a retryOnSignal() predicate, who answers for the pass the
 * predicate is suspended in: the executor, or whatever the container hands out.
 */
final class ProbingRetryWorkflow extends Workflow
{
    /**
     * @var array{guarded: bool, container_bound: bool}|null
     */
    public static ?array $probe = null;

    public static function reset(): void
    {
        self::$probe = null;
    }

    /**
     * @throws Throwable
     */
    public function handle(string $orderId): mixed
    {
        return $this->action(DeclinableChargeAction::class, $orderId)
            ->retryOnSignal('balance-refilled', when: function (RetryContext $context): bool {
                self::$probe = [
                    'guarded' => app(FlowExecutor::class)->isDecidingRun($context->runId),
                    'container_bound' => self::isBound(app(FlowRuntime::class)),
                ];

                return true;
            })
            ->run();
    }

    private static function isBound(FlowRuntime $runtime): bool
    {
        try {
            $runtime->run();
        } catch (MissingFlowContextException) {
            return false;
        }

        return true;
    }
}
