<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * A failing step that is optional only by virtue of its #[ContinueOnFailure]
 * attribute, then a normal step. The flow must complete (the failure is swallowed
 * and run() returns the fallback) when the attribute is honoured.
 */
final class OptionalAttributedWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(OptionalAttributedAction::class)->fallbackValueOnFail('fallback')->run();

        $this->action(MakeValueAction::class, 'after')->run();
    }
}
