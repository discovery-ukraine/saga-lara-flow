<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\SideEffect;

final readonly class SideEffectReused
{
    public function __construct(
        public SideEffect $sideEffect,
    ) {}
}
