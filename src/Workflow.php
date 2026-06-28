<?php

namespace DiscoveryUkraine\SagaLaraFlow;

use Closure;
use DateTimeInterface;
use DiscoveryUkraine\SagaLaraFlow\Builders\ActionBuilder;
use DiscoveryUkraine\SagaLaraFlow\Builders\SignalWaitBuilder;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FlowSuspended;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\InternalFlowControl;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRuntime;
use DiscoveryUkraine\SagaLaraFlow\Runtime\SideEffectStore;
use DiscoveryUkraine\SagaLaraFlow\Runtime\SignalWaiter;
use Illuminate\Contracts\Container\BindingResolutionException;
use Illuminate\Contracts\Container\CircularDependencyException;
use Illuminate\Support\Traits\Macroable;
use Throwable;

/**
 * Base class for user-defined workflows. Author a deterministic handle() method;
 * the runtime drives it via exception-based suspension and replay.
 *
 * All operations are instance methods — no global helpers, no static state.
 * Sequential actions are available now; signals, child workflows, parallel
 * groups, sagas, and side effects are introduced in later phases.
 */
abstract class Workflow
{
    use Macroable;

    public function __construct(
        protected readonly FlowRuntime $runtime,
    ) {}

    /**
     * Begin a compensatable action step. The returned builder records and
     * replays the action by its (flow_run_id, sequence) identity.
     */
    public function action(string $actionClass, mixed ...$arguments): ActionBuilder
    {
        return new ActionBuilder(
            $this->runtime,
            $actionClass,
            array_values($arguments),
        );
    }

    /**
     * Capture a nondeterministic value (now(), a uuid, randomness) exactly once.
     * The factory runs on the first pass; every later replay returns the stored
     * value by its (flow_run_id, sequence) identity without re-running it. Wrap
     * any nondeterminism you branch on in here to keep handle() deterministic.
     *
     * @throws HistoryContractMismatchException
     * @throws BindingResolutionException
     * @throws CircularDependencyException
     */
    public function sideEffect(string $key, Closure $factory): mixed
    {
        return app(SideEffectStore::class)->resolve($this->runtime, $key, $factory);
    }

    /**
     * Wait for an external signal, identified by the operation's (flow_run_id,
     * sequence) ordinal. Returns the signal payload once delivered: if a matching
     * signal already arrived it resolves inline, otherwise the flow suspends and
     * resumes when the signal is delivered. Replays return the same payload.
     *
     * $timeout is accepted for API stability (§4) but is a no-op until Phase 8
     * (monitor); it is neither persisted nor enforced yet.
     *
     * @throws HistoryContractMismatchException
     * @throws FlowSuspended
     */
    public function awaitSignal(string $name, ?DateTimeInterface $timeout = null): mixed
    {
        return app(SignalWaiter::class)->await($this->runtime, $name);
    }

    /**
     * Fluent form of awaitSignal: $this->signal('name')->timeoutAfter($when)->wait().
     */
    public function signal(string $name): SignalWaitBuilder
    {
        return new SignalWaitBuilder($this->runtime, $name);
    }

    /**
     * Attach a queryable tag to the current run. Idempotent across replays.
     */
    public function tag(string $key, string|int|null $value = null): void
    {
        $this->runtime->run()->tags()->updateOrCreate(
            ['key' => $key],
            ['value' => $value === null ? null : (string) $value],
        );
    }

    public function runId(): string
    {
        return $this->runtime->run()->id;
    }

    public function flowName(): ?string
    {
        return $this->runtime->run()->workflow_name;
    }

    public function version(): ?string
    {
        return $this->runtime->run()->workflow_version;
    }

    public function parentRunId(): ?string
    {
        return $this->runtime->run()->parent_id;
    }

    /**
     * True when the throwable is an internal suspension/control signal. Use this
     * to rethrow from any catch (Throwable) block in handle():
     * `if ($this->isFlowControl($e)) { throw $e; }`.
     */
    protected function isFlowControl(Throwable $e): bool
    {
        return $e instanceof InternalFlowControl;
    }
}
