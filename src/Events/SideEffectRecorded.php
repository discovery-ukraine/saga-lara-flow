<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\SideEffect;

final readonly class SideEffectRecorded
{
    public function __construct(
        public SideEffect $sideEffect,
    ) {}
}
