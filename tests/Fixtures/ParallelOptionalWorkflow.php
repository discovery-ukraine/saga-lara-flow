<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * A parallel block whose middle step is optional and fails: the block does not
 * fail. The optional slot resolves to its fallback while the other steps complete,
 * and the flow finishes successfully.
 */
final class ParallelOptionalWorkflow extends Workflow
{
    /**
     * @return array{results: list<mixed>}
     */
    public function handle(): array
    {
        $results = $this->parallel()
            ->action(MakeValueAction::class, 'a')
            ->optionalAction(ThrowingAction::class)->fallbackValueOnFail('skipped')
            ->action(MakeValueAction::class, 'c')
            ->run();

        return ['results' => $results];
    }
}
