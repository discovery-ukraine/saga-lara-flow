<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Throwable;

/**
 * Parks on a retry signal the test names, either on a step or on a child, so runs parked
 * on different signals can sit side by side. Both the step and the child run
 * FlakyPaymentAction: reset its failures to decide how many attempts fail. The first step
 * is compensated, so an expired run rolls back through Cancelling. $hold names an ordinary
 * signal the run waits on once the retried work is done.
 */
final class NamedRetryWorkflow extends Workflow
{
    /**
     * @throws Throwable
     */
    public function handle(string $signal, bool $child = false, ?string $hold = null, ?int $waitSeconds = null): void
    {
        $this->action(MakeValueAction::class, 'created')
            ->compensateWith(UndoAction::class, 'created')
            ->run();

        if ($child) {
            $this->child(CountedActionWorkflow::class, [$signal])
                ->retryOnSignal($signal, waitSeconds: $waitSeconds)
                ->run();
        } else {
            $this->action(FlakyPaymentAction::class, $signal)
                ->retryOnSignal($signal, waitSeconds: $waitSeconds)
                ->run();
        }

        $this->action(MakeValueAction::class, 'shipped')->run();

        if ($hold !== null) {
            $this->awaitSignal($hold);
        }
    }
}
