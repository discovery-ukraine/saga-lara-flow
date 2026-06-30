<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * A standalone optional action that always fails: its failure must NOT fail the
 * flow. run() returns the fallback and the flow completes with the later step's
 * result, proving continueOnFailure()/fallback() let the workflow carry on.
 *
 * @return array{optional: string, after: array{label: string}}
 */
final class OptionalFallbackWorkflow extends Workflow
{
    /**
     * @return array{optional: mixed, after: array{label: string}}
     */
    public function handle(): array
    {
        $optional = $this->action(ThrowingAction::class)
            ->continueOnFailure()
            ->fallbackValueOnFail('skipped')
            ->run();

        $after = $this->action(MakeValueAction::class, 'after')->run();

        return ['optional' => $optional, 'after' => $after];
    }
}
