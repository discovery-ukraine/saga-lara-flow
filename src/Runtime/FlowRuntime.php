<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\MissingFlowContextException;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

/**
 * Per-execution, scoped state for the workflow being driven: the current run,
 * the run mode, and the step sequence. Holds NO static mutable state so that
 * two runs driven back-to-back in the same process (Octane, sync queue) never
 * leak context into one another.
 *
 * One instance is bound per drive() pass; reset() zeroes the sequence at the
 * start of every replay and clear() wipes it in the executor's "finally" block.
 */
final class FlowRuntime
{
    private ?FlowRun $flowRun = null;

    private RunMode $mode = RunMode::Queued;

    public function __construct(
        private readonly StepSequence $sequence = new StepSequence,
    ) {}

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

    public function nextSequence(): int
    {
        return $this->sequence->next();
    }

    /**
     * Start a replay pass: rewind the sequence counter to 0. Saga stack and
     * pending parallel state will be rewound here too in later phases.
     */
    public function reset(): void
    {
        $this->sequence->reset();
    }

    /**
     * Fully detach the run after a drive() pass completes (executor finally).
     */
    public function clear(): void
    {
        $this->flowRun = null;
        $this->sequence->reset();
    }
}
