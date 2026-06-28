<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

/**
 * Monotonic, deterministic counter for the operations performed during a single
 * handle() invocation. It is reset to 0 at the start of every replay, so the
 * same control flow always assigns the same sequence to the same operation.
 *
 * @see FlowRuntime
 */
final class StepSequence
{
    private int $next = 0;

    /**
     * Return the current ordinal and advance the counter. The first call in a
     * pass returns 0.
     */
    public function next(): int
    {
        return $this->next++;
    }

    /**
     * The ordinal that the next call to next() would return, i.e. the number of
     * operations assigned so far in this pass.
     */
    public function current(): int
    {
        return $this->next;
    }

    public function reset(): void
    {
        $this->next = 0;
    }
}
