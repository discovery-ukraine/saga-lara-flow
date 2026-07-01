<?php

namespace DiscoveryUkraine\SagaLaraFlow\Concerns\Workflow;

use Closure;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FlowSuspended;
use DiscoveryUkraine\SagaLaraFlow\Runtime\SideEffectStore;
use Illuminate\Contracts\Container\BindingResolutionException;
use Illuminate\Contracts\Container\CircularDependencyException;

/**
 * The sideEffect() DSL: capturing nondeterministic values once, then replaying them.
 */
trait InteractsWithSideEffects
{
    /**
     * Capture a nondeterministic value (now(), a uuid, randomness) exactly once.
     * The factory runs on the first pass; every later replay returns the stored
     * value by its (flow_run_id, sequence) identity without re-running it. Wrap
     * any nondeterminism you branch on in here to keep handle() deterministic.
     *
     * @throws HistoryContractMismatchException handle() diverged from recorded history
     * @throws BindingResolutionException
     * @throws CircularDependencyException
     * @throws FlowSuspended internal control signal — never catch this in handle()
     */
    public function sideEffect(string $key, Closure $factory): mixed
    {
        return app(SideEffectStore::class)->resolve($this->runtime, $key, $factory);
    }
}
