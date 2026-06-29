<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Action;
use RuntimeException;

/**
 * Compensation that always fails, to exercise the Stop/Continue policy.
 */
final class FailingUndoAction extends Action
{
    public function handle(string $label): never
    {
        throw new RuntimeException('undo-failed:'.$label);
    }
}
