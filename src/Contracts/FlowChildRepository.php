<?php

namespace DiscoveryUkraine\SagaLaraFlow\Contracts;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use Illuminate\Database\Eloquent\Collection;

interface FlowChildRepository
{
    /**
     * The child link recorded at a parent's (parent_flow_run_id, sequence) ordinal.
     */
    public function find(string $parentFlowRunId, int $sequence): ?FlowChild;

    /**
     * Child links of a parent whose child run is not yet terminal.
     *
     * @return Collection<int, FlowChild>
     */
    public function active(string $parentFlowRunId): Collection;
}
