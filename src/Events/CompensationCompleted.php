<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\CompensationRun;

final readonly class CompensationCompleted
{
    public function __construct(
        public CompensationRun $compensationRun,
    ) {}
}
