<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;

final readonly class FlowSignalConsumed
{
    public function __construct(
        public FlowSignal $signal,
    ) {}
}
