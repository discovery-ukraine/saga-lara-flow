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
     * Persist a new flow run together with its tags.
     *
     * @param  array<string, mixed>  $attributes
     * @param  array<int, array{key: string, value: ?string}>  $tags
     */
    public function create(array $attributes, array $tags = []): FlowRun;

    public function find(string $id): ?FlowRun;

    /**
     * @throws FlowNotFoundException
     */
    public function findOrFail(string $id): FlowRun;

    /**
     * Non-terminal runs (Running/Waiting) whose expires_at deadline has passed and
     * whose hold-off window is open, capped at $limit. Ordered by the longest wait the
     * monitor can act on: the hold-off window where a run has one, the deadline
     * otherwise. Used by the monitor to expire stuck runs.
     *
     * @return iterable<int, FlowRun>
     */
    public function dueForExpiration(int $limit): iterable;

    /**
     * Waiting runs older than the grace window with no in-flight blocker (no
     * Pending/Running action and no Waiting signal) whose repair window is open and
     * attempts are not exhausted, oldest first, capped at $limit. Used by the doctor
     * to re-wake a flow whose resume was lost — replay then decides.
     *
     * @return iterable<int, FlowRun>
     */
    public function dueForRepair(int $limit, int $graceSeconds, int $maxAttempts): iterable;
}
