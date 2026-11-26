<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use Illuminate\Contracts\Events\ShouldDispatchAfterCommit;

/**
 * A failed step parked on its retryOnSignal() signal; one per parking, after it commits.
 */
final readonly class ActionAwaitingRetry implements ShouldDispatchAfterCommit
{
    public function __construct(
        public ActionRun $actionRun,
        public string $signal,
    ) {}
}
