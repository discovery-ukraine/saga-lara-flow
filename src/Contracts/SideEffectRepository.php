<?php

namespace DiscoveryUkraine\SagaLaraFlow\Contracts;

use DiscoveryUkraine\SagaLaraFlow\Models\SideEffect;

interface SideEffectRepository
{
    public function find(string $flowRunId, int $sequence): ?SideEffect;
}
