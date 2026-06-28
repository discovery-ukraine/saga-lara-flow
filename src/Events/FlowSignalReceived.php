<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;

final readonly class FlowSignalReceived
{
    public function __construct(
        public FlowSignal $signal,
    ) {}
}
