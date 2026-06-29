<?php

namespace DiscoveryUkraine\SagaLaraFlow\Builders;

use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ChildWorkflowManager;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRuntime;
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

    /**
     * @param  array<int, mixed>  $arguments
     */
    public function __construct(
        private readonly FlowRuntime $runtime,
        private readonly string $workflowClass,
        private readonly array $arguments,
    ) {
        $this->closePolicy = config('saga-lara-flow.children.default_close_policy');
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

    /**
     * Await the child and return its result.
     *
     * @throws Throwable
     */
    public function run(): mixed
    {
        return app(ChildWorkflowManager::class)->await(
            $this->runtime,
            $this->workflowClass,
            $this->arguments,
            $this->closePolicy,
            $this->continueParentOnFailure,
        );
    }
}
