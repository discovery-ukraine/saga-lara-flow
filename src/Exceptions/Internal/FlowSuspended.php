<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal;

/**
 * Thrown by the runtime when a workflow step cannot be resolved from stored
 * history yet. The FlowExecutor catches it to suspend (queued) or replay
 * (sync, when inlineResolved) the run. Never catch this in workflow code.
 */
final class FlowSuspended extends InternalFlowControl
{
    /**
     * @param  string  $reason  What suspended the flow: 'action' | 'signal' | 'child' | 'parallel'.
     * @param  int  $sequence  The (flow_run_id, sequence) ordinal of the suspending step.
     * @param  array<string, mixed>  $context  Optional diagnostic context.
     * @param  bool  $inlineResolved  Sync mode: the step was executed inline and the loop should replay.
     */
    public function __construct(
        public readonly string $reason,
        public readonly int $sequence,
        public readonly array $context = [],
        public readonly bool $inlineResolved = false,
    ) {
        parent::__construct('saga-lara-flow: flow intentionally suspended; do not catch.');
    }
}
