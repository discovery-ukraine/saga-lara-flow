<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;

/**
 * The doctor re-dispatched a stuck Pending action whose RunActionJob was lost.
 */
final readonly class ActionRedispatched
{
    public function __construct(
        public ActionRun $actionRun,
    ) {}
}
