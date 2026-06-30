<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Three actions run as one parallel block. handle() returns their results so a
 * test can assert they are ordered by declaration regardless of completion order.
 */
final class ParallelEchoWorkflow extends Workflow
{
    /**
     * @return array{results: list<mixed>}
     */
    public function handle(): array
    {
        $results = $this->parallel()
            ->action(MakeValueAction::class, 'a')
            ->action(MakeValueAction::class, 'b')
            ->action(MakeValueAction::class, 'c')
            ->run();

        return ['results' => $results];
    }
}
