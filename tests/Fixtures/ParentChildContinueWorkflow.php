<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Parent that awaits a failing child with continueParentOnFailure(): the child rolls
 * itself back (undo:child-a) and child()->run() returns null, so the parent carries
 * on to completion. Expected log: undo:child-a only.
 */
final class ParentChildContinueWorkflow extends Workflow
{
    /**
     * @return array{childResult: mixed, after: string}
     */
    public function handle(): array
    {
        $result = $this->child(FailingChildWorkflow::class)->continueParentOnFailure()->run();

        $after = $this->action(MakeValueAction::class, 'after')->run();

        return ['childResult' => $result, 'after' => $after['label']];
    }
}
