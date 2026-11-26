<?php

namespace DiscoveryUkraine\SagaLaraFlow\Contracts;

use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowNotFoundException;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

/**
 * @internal This contract is an implementation seam for the package's own
 * runtime, not a public extension point. Methods may be added to it in a minor
 * release; swap behaviour through config('saga-lara-flow.models.*') instead.
 */
interface FlowRepository
{
    /**
     * @param  array<string, mixed>  $attributes
     * @param  array<int, array{key: string, value: ?string}>  $tags
     */
    public function create(array $attributes, array $tags = []): FlowRun;

    public function find(string $id): ?FlowRun;

    /**
     * Read from the writer, for a caller deciding whether to act on the run.
     */
    public function findOnWriter(string $id): ?FlowRun;

    /**
     * @throws FlowNotFoundException
     */
    public function findOrFail(string $id): FlowRun;

    /**
     * Running or Waiting runs past their deadline with the hold-off window open, by the later of the two.
     *
     * @return iterable<int, FlowRun>
     */
    public function dueForExpiration(int $limit): iterable;

    /**
     * Waiting runs past the grace window with no Pending/Running step and no Waiting signal.
     *
     * @return iterable<int, FlowRun>
     */
    public function dueForRepair(int $limit, int $graceSeconds, int $maxAttempts): iterable;

    /**
     * Pending runs past the grace window, a child of a closed parent included.
     *
     * @return iterable<int, FlowRun>
     */
    public function dueForStartRepair(int $limit, int $graceSeconds, int $maxAttempts): iterable;

    /**
     * Stalled rollbacks to Expired with the hold-off window open.
     *
     * @return iterable<int, FlowRun>
     */
    public function dueForRollbackRecovery(int $limit, int $graceSeconds): iterable;

    /**
     * Stalled rollbacks to Failed or Cancelled within the repair budget.
     *
     * @return iterable<int, FlowRun>
     */
    public function dueForRollbackRepair(int $limit, int $graceSeconds, int $maxAttempts): iterable;
}
