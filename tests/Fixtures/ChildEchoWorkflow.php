<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Child workflow that runs one action and returns its result. Used to prove a
 * parent awaits a child and receives its result.
 */
final class ChildEchoWorkflow extends Workflow
{
    /**
     * @return array{label: string}
     */
    public function handle(string $label): array
    {
        return $this->action(MakeValueAction::class, $label)->run();
    }
}
