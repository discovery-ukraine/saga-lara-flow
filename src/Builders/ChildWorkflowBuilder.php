<?php

namespace DiscoveryUkraine\SagaLaraFlow\Builders;

use Closure;
use DateTimeInterface;
use DiscoveryUkraine\SagaLaraFlow\Data\ChildSchedule;
use DiscoveryUkraine\SagaLaraFlow\Data\CompensationDefinition;
use DiscoveryUkraine\SagaLaraFlow\Data\SignalRetry;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationFailurePolicy;
use DiscoveryUkraine\SagaLaraFlow\Retry\RetryContext;
use DiscoveryUkraine\SagaLaraFlow\Retry\RetryPolicy;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ChildWorkflowManager;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRuntime;
use DiscoveryUkraine\SagaLaraFlow\Support\AttributeReader;
use Throwable;

/**
 * Fluent builder for a child workflow step. run() awaits the child: the parent
 * suspends until the child reaches a terminal state, then resolves the child's
 * result (or surfaces its failure) by the operation's (flow_run_id, sequence)
 * identity — the same replay seam used by action()->run() and awaitSignal().
 *
 * closePolicy() decides what happens to a still-active child when THIS parent
 * becomes terminal (Abandon/Cancel/Fail). continueParentOnFailure() decides what happens
 * to the parent when the child fails: by default the failure propagates (the parent
 * compensates and fails too); with it set, the child rolls itself back and run()
 * returns null so the parent can carry on.
 */
class ChildWorkflowBuilder
{
    private ChildClosePolicy $closePolicy;

    private bool $continueParentOnFailure = false;

    private ?DateTimeInterface $expiresAt = null;

    private ?CompensationDefinition $compensation = null;

    private ?CompensationFailurePolicy $compensationFailurePolicy = null;

    private ?SignalRetry $retry = null;

    /**
     * @param  array<int, mixed>  $arguments
     */
    public function __construct(
        private readonly FlowRuntime $runtime,
        private readonly string $workflowClass,
        private readonly array $arguments,
    ) {
        // Precedence: an explicit ->closePolicy() (below) wins; otherwise the
        // child's #[ChildPolicy] attribute, then the configured default.
        $this->closePolicy = app(AttributeReader::class)->childPolicy($workflowClass)
            ?? config('saga-lara-flow.children.default_close_policy');
    }

    public function closePolicy(ChildClosePolicy $policy): static
    {
        $this->closePolicy = $policy;

        return $this;
    }

    public function continueParentOnFailure(bool $continue = true): static
    {
        $this->continueParentOnFailure = $continue;

        return $this;
    }

    public function expiresAt(?DateTimeInterface $expiresAt): static
    {
        $this->expiresAt = $expiresAt;

        return $this;
    }

    public function compensateWith(string|Closure $compensation, mixed ...$arguments): static
    {
        $this->compensation = $compensation instanceof Closure
            ? CompensationDefinition::forClosure($compensation)
            : CompensationDefinition::forClass($compensation, array_values($arguments));

        return $this;
    }

    public function onCompensationFailure(CompensationFailurePolicy $policy): static
    {
        $this->compensationFailurePolicy = $policy;

        return $this;
    }

    /**
     * Park the parent on $signal when the child fails or expires; delivering it to the
     * parent starts the child again at the same ordinal. See ActionBuilder::retryOnSignal().
     *
     * @param  list<class-string<Throwable>>|null  $only
     * @param  ?Closure(RetryContext): bool  $when
     */
    public function retryOnSignal(
        RetryPolicy|string $signal,
        ?int $maxRetries = null,
        ?int $waitSeconds = null,
        ?array $only = null,
        ?Closure $when = null,
    ): static {
        $this->retry = SignalRetry::for($signal, $maxRetries, $waitSeconds, $only, $when);

        return $this;
    }

    /**
     * Await the child and return its result.
     *
     * @throws Throwable
     */
    public function run(): mixed
    {
        return app(ChildWorkflowManager::class)->await($this->runtime, new ChildSchedule(
            workflowClass: $this->workflowClass,
            arguments: $this->arguments,
            closePolicy: $this->closePolicy,
            continueParentOnFailure: $this->continueParentOnFailure,
            expiresAt: $this->expiresAt,
            compensation: $this->compensation,
            compensationFailurePolicy: $this->compensationFailurePolicy,
            retry: $this->retry,
        ));
    }
}
