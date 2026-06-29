<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\CompensationRun;
use Throwable;

final readonly class CompensationFailed
{
    public function __construct(
        public CompensationRun $compensationRun,
        public Throwable $exception,
    ) {}
}
