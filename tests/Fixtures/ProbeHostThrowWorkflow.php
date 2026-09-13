<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionFailedException;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Throwable;

/**
 * Completes two compensatable steps and parks on a signal. Once $throw names an
 * exception class, handle() raises one of that class itself between the two steps —
 * the shape a workflow reaches when it re-raises an engine exception caught from a
 * helper, or branches on something that changed between the run and the rollback.
 * With $viaFactory it builds that instance through the class's own named constructor
 * instead of constructing one directly.
 */
final class ProbeHostThrowWorkflow extends Workflow
{
    /**
     * @var class-string<Throwable>|null
     */
    public static ?string $throw = null;

    public static bool $viaFactory = false;

    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')
            ->compensateWith(UndoAction::class, 'a')
            ->run();

        if (self::$viaFactory) {
            throw ActionFailedException::forAction(MakeValueAction::class, 1, 'the host raised this one itself');
        }

        if (self::$throw !== null) {
            throw new (self::$throw)('the host raised this one itself');
        }

        $this->action(MakeValueAction::class, 'b')
            ->compensateWith(UndoAction::class, 'b')
            ->run();

        $this->awaitSignal('go');
    }

    public static function reset(): void
    {
        self::$throw = null;
        self::$viaFactory = false;
    }
}
