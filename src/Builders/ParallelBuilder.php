<?php

namespace DiscoveryUkraine\SagaLaraFlow\Builders;

use DiscoveryUkraine\SagaLaraFlow\Enums\ParallelFailurePolicy;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRuntime;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ParallelRunner;
use Throwable;

/**
 * A parallel block: its steps are dispatched together and the flow continues only
 * once they all reach a terminal state, returning their results in declaration
 * order. failFast() fails as soon as one step fails (cancelling pending siblings);
 * waitAllThenFail() lets every step settle first. Completed steps share one parallel
 * group id so they roll back together as a single compensation level.
 */
final class ParallelBuilder
{
    private ParallelFailurePolicy $policy;

    /**
     * @var list<ParallelStepBuilder>
     */
    private array $steps = [];

    public function __construct(
        private readonly FlowRuntime $runtime,
    ) {
        $this->policy = config('saga-lara-flow.parallel.default_failure_policy');
    }

    public function action(string $actionClass, mixed ...$arguments): ParallelStepBuilder
    {
        $step = new ParallelStepBuilder($this, $actionClass, array_values($arguments));

        $this->steps[] = $step;

        return $step;
    }

    public function optionalAction(string $actionClass, mixed ...$arguments): ParallelStepBuilder
    {
        return $this->action($actionClass, ...$arguments)->continueOnFailure();
    }

    public function failFast(): self
    {
        $this->policy = ParallelFailurePolicy::FailFast;

        return $this;
    }

    public function waitAllThenFail(): self
    {
        $this->policy = ParallelFailurePolicy::WaitAllThenFail;

        return $this;
    }

    /**
     * @return list<mixed>
     *
     * @throws Throwable
     */
    public function run(): array
    {
        return app(ParallelRunner::class)->run($this->runtime, $this->steps, $this->policy);
    }
}
