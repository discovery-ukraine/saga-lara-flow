<?php

namespace DiscoveryUkraine\SagaLaraFlow\Contracts;

use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;

interface ActionRunRepository
{
    public function find(string $flowRunId, int $sequence): ?ActionRun;
}
