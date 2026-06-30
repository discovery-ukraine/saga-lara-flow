<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

/**
 * Result of one prune pass: how many terminal runs and how many related rows
 * (across the child tables) were deleted — or, for a dry run, would be deleted.
 */
final readonly class FlowPruneReport
{
    public function __construct(
        public int $runs = 0,
        public int $relatedRows = 0,
    ) {}

    /**
     * @return array{runs: int, related_rows: int}
     */
    public function toArray(): array
    {
        return [
            'runs' => $this->runs,
            'related_rows' => $this->relatedRows,
        ];
    }
}
