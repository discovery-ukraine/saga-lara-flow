<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Action;
use RuntimeException;

/**
 * Action that always fails with a business error.
 */
final class ThrowingAction extends Action
{
    public function handle(): never
    {
        throw new RuntimeException('boom');
    }
}
