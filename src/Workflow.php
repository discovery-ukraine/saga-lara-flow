<?php

namespace DiscoveryUkraine\SagaLaraFlow;

use DiscoveryUkraine\SagaLaraFlow\Builders\ActionBuilder;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\InternalFlowControl;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionDispatcher;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRuntime;
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
            app(ActionDispatcher::class),
            $actionClass,
            array_values($arguments),
        );
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
