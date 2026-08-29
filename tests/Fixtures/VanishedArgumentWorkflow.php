<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;
use RuntimeException;

/**
 * Two compensatable steps, then a wait. The second step's argument is read from
 * something outside the run — the stand-in for a record the workflow looks up while
 * building its steps. Clearing it makes a later replay throw where the original pass
 * did not, which is what a rollback started after the world moved on actually meets.
 */
final class VanishedArgumentWorkflow extends Workflow
{
    public static ?string $label = 'b';

    public static function reset(): void
    {
        self::$label = 'b';
    }

    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')
            ->compensateWith(UndoAction::class, 'a')
            ->run();

        $this->action(MakeValueAction::class, $this->label())
            ->compensateWith(UndoAction::class, 'b')
            ->run();

        $this->awaitSignal('go');
    }

    private function label(): string
    {
        return self::$label ?? throw new RuntimeException('the order this run was built from is gone');
    }
}
