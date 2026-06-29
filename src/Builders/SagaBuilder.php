<?php

namespace DiscoveryUkraine\SagaLaraFlow\Builders;

use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationFailurePolicy;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRuntime;
use Throwable;

/**
 * Explicit saga group: equivalent power to action-level compensation plus group
 * policies. Steps run in order (each its own sequence) and register their
 * compensations onto the same saga stack; on rollback the group's compensations
 * honour the group onCompensationFailure() and, when compensateInParallel() (or
 * config sagas.parallel_compensation) is set, roll back together as one level.
 */
final class SagaBuilder
{
    private ?CompensationFailurePolicy $compensationFailurePolicy = null;

    private ?bool $compensateOnSelfFailure = null;

    private bool $parallel = false;

    /**
     * @var list<SagaStepBuilder>
     */
    private array $steps = [];

    public function __construct(
        private readonly FlowRuntime $runtime,
    ) {}

    public function onCompensationFailure(CompensationFailurePolicy $policy): self
    {
        $this->compensationFailurePolicy = $policy;

        return $this;
    }

    public function compensateInParallel(): self
    {
        $this->parallel = true;

        return $this;
    }

    /**
     * Compensate every step in this group even if the step itself fails (not only
     * completed steps). Steps may still override per step. See ActionBuilder::compensateStepOnSelfFailure().
     */
    public function compensateStepOnSelfFailure(bool $compensate = true): self
    {
        $this->compensateOnSelfFailure = $compensate;

        return $this;
    }

    public function step(string $actionClass, mixed ...$arguments): SagaStepBuilder
    {
        $step = new SagaStepBuilder($this, $actionClass, array_values($arguments));

        $this->steps[] = $step;

        return $step;
    }

    /**
     * @return list<mixed>
     *
     * @throws Throwable
     */
    public function run(): array
    {
        $parallel = $this->parallel || (bool) config('saga-lara-flow.sagas.parallel_compensation');

        $parallelGroupId = $parallel ? $this->runtime->nextSagaGroupId() : null;

        $results = [];

        foreach ($this->steps as $step) {
            $results[] = $step->execute(
                $this->runtime,
                $this->compensationFailurePolicy,
                $this->compensateOnSelfFailure,
                $parallelGroupId
            );
        }

        return $results;
    }
}
