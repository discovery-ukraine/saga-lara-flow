<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\MissingFlowContextException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\RetryPolicyReentryException;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Throwable;
use WeakMap;

/**
 * Per-execution state for the workflow being driven: the current run, the run
 * mode, and the step sequence. Holds NO static mutable state, and the executor
 * makes one for every pass it drives, so neither two runs driven back-to-back in
 * the same process (Octane, sync queue) nor a pass driven inside another leak
 * context into one another.
 *
 * reset() zeroes the sequence at the start of every replay; clear() unbinds the
 * run in the executor's "finally" block.
 */
final class FlowRuntime
{
    private ?FlowRun $flowRun = null;

    private RunMode $mode = RunMode::Queued;

    private int $sagaGroup = 0;

    private bool $collecting = false;

    private bool $deciding = false;

    /**
     * @var WeakMap<Throwable, true>
     */
    private WeakMap $raised;

    public function __construct(
        private readonly StepSequence $sequence = new StepSequence,
        private readonly SagaStack $sagaStack = new SagaStack,
    ) {
        $this->raised = new WeakMap;
    }

    /**
     * Record a throw this pass is raising, and hand it back to be thrown.
     *
     * @template TThrow of Throwable
     *
     * @param  TThrow  $ending
     * @return TThrow
     */
    public function raising(Throwable $ending): Throwable
    {
        $this->raised[$ending] = true;

        return $ending;
    }

    public function raised(Throwable $ending): bool
    {
        return isset($this->raised[$ending]);
    }

    public function bind(FlowRun $flowRun, RunMode $mode): void
    {
        $this->flowRun = $flowRun;
        $this->mode = $mode;
    }

    public function run(): FlowRun
    {
        return $this->flowRun ?? throw new MissingFlowContextException(
            'No flow run is bound to the runtime. Workflow operations must run inside FlowExecutor::drive().'
        );
    }

    public function mode(): RunMode
    {
        return $this->mode;
    }

    /**
     * @throws RetryPolicyReentryException
     */
    public function nextSequence(): int
    {
        // Every seam takes its ordinal here first, so one check covers them all.
        if ($this->deciding) {
            throw RetryPolicyReentryException::for('a workflow operation');
        }

        return $this->sequence->next();
    }

    /**
     * The compensation stack for this execution. Rebuilt deterministically each
     * pass: reset() empties it, ActionBuilder pushes a step's compensation (every
     * completed step, plus an opt-in failed one), and SagaRunner reads it (LIFO)
     * when a failure triggers rollback.
     */
    public function sagaStack(): SagaStack
    {
        return $this->sagaStack;
    }

    /**
     * Deterministic id for the next saga() group in this pass, used to mark its
     * steps as one parallel rollback level.
     */
    public function nextSagaGroupId(): int
    {
        return $this->sagaGroup++;
    }

    /**
     * Enter "compensation-only" planning: seams resolve completed steps (to rebuild
     * the saga stack) but never start new work — they suspend at the live frontier.
     */
    public function beginCollecting(): void
    {
        $this->collecting = true;
    }

    public function endCollecting(): void
    {
        $this->collecting = false;
    }

    public function isCollecting(): bool
    {
        return $this->collecting;
    }

    /**
     * Enter a retryOnSignal() decision: while the caller's predicate runs, no seam
     * may consume an ordinal. See RetryPolicyReentryException for why.
     */
    public function beginDeciding(): void
    {
        $this->deciding = true;
    }

    public function endDeciding(): void
    {
        $this->deciding = false;
    }

    /**
     * Whether a retryOnSignal() decision is in flight on this pass.
     */
    public function isDeciding(): bool
    {
        return $this->deciding;
    }

    /**
     * Whether the run being decided is this one. A predicate may touch other runs;
     * not the one whose parking the seam writes the moment it returns.
     */
    public function isDecidingRun(string $flowRunId): bool
    {
        return $this->deciding && $this->flowRun?->id === $flowRunId;
    }

    /**
     * Start a replay pass: rewind the sequence counter and the saga stack/group
     * counter so the pass rebuilds them deterministically from stored history.
     */
    public function reset(): void
    {
        $this->sequence->reset();
        $this->sagaStack->reset();
        $this->sagaGroup = 0;
        $this->collecting = false;
        $this->deciding = false;
        $this->raised = new WeakMap;
    }

    /**
     * Detach the run after a drive() pass completes (executor finally). The saga
     * stack is intentionally left intact so failAndCompensate() can read the
     * compensations gathered by the failing pass after this clear() runs.
     */
    public function clear(): void
    {
        $this->flowRun = null;
        $this->sequence->reset();
    }
}
