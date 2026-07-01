<?php

namespace DiscoveryUkraine\SagaLaraFlow;

use DiscoveryUkraine\SagaLaraFlow\Concerns\Workflow\InteractsWithActions;
use DiscoveryUkraine\SagaLaraFlow\Concerns\Workflow\InteractsWithChildren;
use DiscoveryUkraine\SagaLaraFlow\Concerns\Workflow\InteractsWithParallelism;
use DiscoveryUkraine\SagaLaraFlow\Concerns\Workflow\InteractsWithSagas;
use DiscoveryUkraine\SagaLaraFlow\Concerns\Workflow\InteractsWithSideEffects;
use DiscoveryUkraine\SagaLaraFlow\Concerns\Workflow\InteractsWithSignals;
use DiscoveryUkraine\SagaLaraFlow\Concerns\Workflow\ProvidesFlowMetadata;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\InternalFlowControl;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRuntime;
use Illuminate\Support\Traits\Macroable;
use Throwable;

/**
 * Base class for user-defined workflows. Author a deterministic handle() method;
 * the runtime drives it via exception-based suspension and replay.
 *
 * All operations are instance methods — no global helpers, no static state. The
 * DSL is grouped into focused concerns (actions, children, parallelism, sagas,
 * signals, side effects, metadata) that all share the injected runtime.
 */
abstract class Workflow
{
    use InteractsWithActions;
    use InteractsWithChildren;
    use InteractsWithParallelism;
    use InteractsWithSagas;
    use InteractsWithSideEffects;
    use InteractsWithSignals;
    use Macroable;
    use ProvidesFlowMetadata;

    public function __construct(
        protected readonly FlowRuntime $runtime,
    ) {}

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
