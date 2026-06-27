<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

/**
 * Minimal workflow used by Phase 1 foundation tests. The base Workflow class
 * and real execution are introduced in Phase 2.
 */
final class TestWorkflow
{
    public function handle(string $orderId = ''): void {}
}
