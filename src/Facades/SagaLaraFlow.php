<?php

namespace DiscoveryUkraine\SagaLaraFlow\Facades;

use Illuminate\Support\Facades\Facade;

/**
 * @see \DiscoveryUkraine\SagaLaraFlow\SagaLaraFlow
 */
class SagaLaraFlow extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return \DiscoveryUkraine\SagaLaraFlow\SagaLaraFlow::class;
    }
}
