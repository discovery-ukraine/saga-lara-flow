<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Parent that starts a child, awaits it, and returns the child's result wrapped.
 */
final class ParentAwaitWorkflow extends Workflow
{
    /**
     * @return array{child: mixed}
     */
    public function handle(): array
    {
        $result = $this->child(ChildEchoWorkflow::class, ['x'])->run();

        return ['child' => $result];
    }
}
