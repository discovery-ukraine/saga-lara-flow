<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationFailurePolicy;
use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Same shape as StopPolicyWorkflow, but the failing compensation (undo b) carries
 * an action-level Continue policy (overriding the config default of Stop). The
 * rollback continues past the failure: undo c runs, undo b fails, undo a still runs.
 */
final class ContinuePolicyWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')
            ->compensateWith(UndoAction::class, 'a')
            ->run();

        $this->action(MakeValueAction::class, 'b')
            ->compensateWith(FailingUndoAction::class, 'b')
            ->onCompensationFailure(CompensationFailurePolicy::Continue)
            ->run();

        $this->action(MakeValueAction::class, 'c')
            ->compensateWith(UndoAction::class, 'c')
            ->run();

        $this->action(ThrowingAction::class)->run();
    }
}
