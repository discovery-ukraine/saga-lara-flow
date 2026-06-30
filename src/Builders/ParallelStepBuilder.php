<?php

namespace DiscoveryUkraine\SagaLaraFlow\Builders;

use Closure;
use DateTimeInterface;
use DiscoveryUkraine\SagaLaraFlow\Data\CompensationDefinition;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationFailurePolicy;
use Throwable;

/**
 * One step of a parallel() block: an action plus its optional flag, fallback, and
 * compensation. compensateWith() (class or closure) registers a compensation that
 * ParallelRunner pushes onto the saga stack with the block's shared parallel group
 * id, so the whole block rolls back together as one level. action()/optionalAction()/
 * failFast()/waitAllThenFail()/run() delegate back to the block so the fluent chain
 * reads as a single concurrent block.
 */
final class ParallelStepBuilder
{
    private ?CompensationDefinition $compensation = null;

    private ?CompensationFailurePolicy $compensationFailurePolicy = null;

    private ?bool $compensateOnSelfFailure = null;

    private bool $optional = false;

    private mixed $fallbackValueOnFail = null;

    private ?DateTimeInterface $expiresAt = null;

    /**
     * @param  array<int, mixed>  $arguments
     */
    public function __construct(
        private readonly ParallelBuilder $parallel,
        private readonly string $actionClass,
        private readonly array $arguments,
    ) {}

    public function compensateWith(string|Closure $compensation, mixed ...$arguments): self
    {
        $this->compensation = $compensation instanceof Closure
            ? CompensationDefinition::forClosure($compensation)
            : CompensationDefinition::forClass($compensation, array_values($arguments));

        return $this;
    }

    public function onCompensationFailure(CompensationFailurePolicy $policy): self
    {
        $this->compensationFailurePolicy = $policy;

        return $this;
    }

    public function compensateStepOnSelfFailure(bool $compensate = true): self
    {
        $this->compensateOnSelfFailure = $compensate;

        return $this;
    }

    public function continueOnFailure(bool $continue = true): self
    {
        $this->optional = $continue;

        return $this;
    }

    public function fallbackValueOnFail(mixed $value): self
    {
        $this->fallbackValueOnFail = $value;

        return $this;
    }

    /**
     * Set a wall-clock deadline for this step (see ActionBuilder::expiresAt()).
     */
    public function expiresAt(?DateTimeInterface $expiresAt): self
    {
        $this->expiresAt = $expiresAt;

        return $this;
    }

    public function action(string $actionClass, mixed ...$arguments): self
    {
        return $this->parallel->action($actionClass, ...$arguments);
    }

    public function optionalAction(string $actionClass, mixed ...$arguments): self
    {
        return $this->parallel->optionalAction($actionClass, ...$arguments);
    }

    public function failFast(): ParallelBuilder
    {
        return $this->parallel->failFast();
    }

    public function waitAllThenFail(): ParallelBuilder
    {
        return $this->parallel->waitAllThenFail();
    }

    /**
     * @return list<mixed>
     *
     * @throws Throwable
     */
    public function run(): array
    {
        return $this->parallel->run();
    }

    public function actionClass(): string
    {
        return $this->actionClass;
    }

    /**
     * @return array<int, mixed>
     */
    public function arguments(): array
    {
        return $this->arguments;
    }

    public function isOptional(): bool
    {
        return $this->optional;
    }

    public function fallbackResult(): mixed
    {
        return $this->fallbackValueOnFail;
    }

    public function expiry(): ?DateTimeInterface
    {
        return $this->expiresAt;
    }

    public function compensation(): ?CompensationDefinition
    {
        return $this->compensation;
    }

    public function compensationFailurePolicy(): ?CompensationFailurePolicy
    {
        return $this->compensationFailurePolicy;
    }

    public function compensateOnSelfFailure(): ?bool
    {
        return $this->compensateOnSelfFailure;
    }
}
