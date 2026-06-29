<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

/**
 * The per-execution compensation stack. Compensatable steps push their compensation
 * here in execution order (every completed step, plus — opt-in via compensateStepOnSelfFailure
 * — a step that failed); it is rebuilt deterministically on every replay (reset() at
 * the start of each pass, push() from ActionBuilder). SagaRunner rolls the entries
 * back in reverse (LIFO).
 *
 * @see FlowRuntime
 */
final class SagaStack
{
    /**
     * @var list<CompensationEntry>
     */
    private array $entries = [];

    public function push(CompensationEntry $entry): void
    {
        $this->entries[] = $entry;
    }

    /**
     * @return list<CompensationEntry>
     */
    public function entries(): array
    {
        return $this->entries;
    }

    public function isEmpty(): bool
    {
        return $this->entries === [];
    }

    public function reset(): void
    {
        $this->entries = [];
    }
}
