<?php

namespace DiscoveryUkraine\SagaLaraFlow\Builders;

use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FlowSuspended;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionDispatcher;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRuntime;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowSuspender;
use DiscoveryUkraine\SagaLaraFlow\Runtime\HistoryContractGuard;
use Throwable;

/**
 * Fluent builder for a single action step. run() is the replay seam: it
 * identifies the step by its (flow_run_id, sequence) ordinal and either returns
 * the stored result, rethrows the stored failure, or schedules/executes the
 * step and suspends the flow.
 *
 * Compensation and optional-failure modifiers are introduced in later phases.
 */
readonly class ActionBuilder
{
    /**
     * @param  array<int, mixed>  $arguments
     */
    public function __construct(
        private FlowRuntime $runtime,
        private string $actionClass,
        private array $arguments,
    ) {}

    /**
     * Resolve this step against stored history, or schedule/run it and suspend.
     *
     * @throws HistoryContractMismatchException
     * @throws Throwable
     */
    public function run(): mixed
    {
        $flowRun = $this->runtime->run();
        $sequence = $this->runtime->nextSequence();

        $existingStep = app(HistoryContractGuard::class)
            ->expectAction($flowRun->id, $sequence, $this->actionClass);

        if ($existingStep !== null) {
            return $this->resolve($existingStep, $sequence);
        }

        $dispatcher = app(ActionDispatcher::class);
        $suspender = app(FlowSuspender::class);

        if ($this->runtime->mode() === RunMode::Sync) {
            $dispatcher->runInline($flowRun, $sequence, $this->actionClass, $this->arguments);
            $suspender->suspendInline('action', $sequence);
        }

        $dispatcher->dispatch($flowRun, $sequence, $this->actionClass, $this->arguments);
        $suspender->suspend('action', $sequence);
    }

    /**
     * @throws FlowSuspended
     */
    private function resolve(ActionRun $step, int $sequence): mixed
    {
        return match ($step->status) {
            ActionStatus::Completed => app(Serializer::class)->deserialize($step->result),
            ActionStatus::Failed => throw ActionFailedException::forAction(
                $this->actionClass,
                $sequence,
                $this->failureMessage($step),
            ),
            // Still in flight (queued job not finished): suspend until resumed.
            default => app(FlowSuspender::class)->suspend('action', $sequence),
        };
    }

    private function failureMessage(ActionRun $step): string
    {
        $exception = $step->exception;

        if (is_array($exception) && isset($exception['message']) && is_string($exception['message'])) {
            return $exception['message'];
        }

        return 'unknown error';
    }
}
