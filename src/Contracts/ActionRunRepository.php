<?php

namespace DiscoveryUkraine\SagaLaraFlow\Contracts;

use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;

interface ActionRunRepository
{
    public function find(string $flowRunId, int $sequence): ?ActionRun;

    /**
     * Non-terminal action steps (Pending/Running) whose expires_at deadline has
     * passed, oldest first, capped at $limit. Used by the monitor to expire stuck
     * actions.
     *
     * @return iterable<int, ActionRun>
     */
    public function dueForExpiration(int $limit): iterable;

    /**
     * Sequential Pending action steps (parallel_group is null) older than the grace
     * window whose repair window is open and attempts are not exhausted, oldest
     * first, capped at $limit. Used by the doctor to re-dispatch a lost RunActionJob.
     *
     * @return iterable<int, ActionRun>
     */
    public function dueForRepair(int $limit, int $graceSeconds, int $maxAttempts): iterable;
}
