<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowTag;
use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Reads back a tag it has just written, straight from the database, and branches on it.
 * Outside the determinism rules, but it holds today because every replay writes the tag
 * before the read, the planning replay included: take that write away and the planning
 * replay reads the later value, takes the other branch and leaves step b out of the plan.
 */
final class TagBranchingWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')
            ->compensateWith(UndoAction::class, 'a')
            ->run();

        $this->tag('phase', 'before-b');

        /** @var class-string<FlowTag> $model */
        $model = config('saga-lara-flow.models.flow_tag');

        $phase = $model::query()
            ->where('flow_run_id', $this->runId())
            ->where('key', 'phase')
            ->value('value');

        if ($phase !== 'before-b') {
            return;
        }

        $this->action(MakeValueAction::class, 'b')
            ->compensateWith(UndoAction::class, 'b')
            ->run();

        $this->tag('phase', 'after-b');

        $this->awaitSignal('hold');
    }
}
