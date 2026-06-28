<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;

final readonly class ActionStarted
{
    public function __construct(
        public ActionRun $actionRun,
    ) {}
}
