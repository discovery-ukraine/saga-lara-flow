<?php

namespace DiscoveryUkraine\SagaLaraFlow\Contracts;

use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowNotFoundException;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

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
     * Non-terminal runs (Running/Waiting) whose expires_at deadline has passed,
     * oldest first, capped at $limit. Used by the monitor to expire stuck runs.
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
