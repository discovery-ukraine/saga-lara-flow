<?php

namespace DiscoveryUkraine\SagaLaraFlow\Builders;

use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FlowSuspended;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionDispatcher;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRuntime;

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
        private ActionDispatcher $dispatcher,
        private string $actionClass,
        private array $arguments,
    ) {}

    /**
     * Resolve this step against stored history, or schedule/run it and suspend.
     *
     * @throws FlowSuspended
     */
    public function run(): mixed
    {
        $flowRun = $this->runtime->run();
        $sequence = $this->runtime->nextSequence();

        $existingStep = $this->findStep($flowRun->id, $sequence);

        if ($existingStep !== null) {
            return $this->resolve($existingStep, $sequence);
        }

        if ($this->runtime->mode() === RunMode::Sync) {
            $this->dispatcher->runInline($flowRun, $sequence, $this->actionClass, $this->arguments);

            throw new FlowSuspended('action', $sequence, inlineResolved: true);
        }

        $this->dispatcher->dispatch($flowRun, $sequence, $this->actionClass, $this->arguments);

        throw new FlowSuspended('action', $sequence);
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
            default => throw new FlowSuspended('action', $sequence),
        };
    }

    private function findStep(string $flowRunId, int $sequence): ?ActionRun
    {
        /** @var class-string<ActionRun> $model */
        $model = config('saga-lara-flow.models.action_run');

        return $model::query()
            ->where('flow_run_id', $flowRunId)
            ->where('sequence', $sequence)
            ->first();
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
