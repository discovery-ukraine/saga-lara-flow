<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use Throwable;

final readonly class ActionFailed
{
    public function __construct(
        public ActionRun $actionRun,
        public Throwable $exception,
    ) {}
}
