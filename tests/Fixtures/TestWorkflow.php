<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Minimal workflow with no steps: it completes immediately when driven.
 */
final class TestWorkflow extends Workflow
{
    public function handle(string $orderId = ''): void {}
}
