<?php

namespace DiscoveryUkraine\SagaLaraFlow\Contracts;

use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowNotFoundException;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\SideEffect;

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

    public function findActionStep(string $flowRunId, int $sequence): ?ActionRun;

    public function findSideEffect(string $flowRunId, int $sequence): ?SideEffect;
}
