<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Tags in bulk before an action suspends the run, so the replay pass re-runs the
 * same tags() call. Used to prove bulk tagging stays idempotent across replays.
 */
final class TaggingReplayWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->tags(['stage' => 'start', 'tenant' => 'acme']);

        $this->action(MakeValueAction::class, 'value')->run();

        $this->tags(['stage' => 'done']);
    }
}
