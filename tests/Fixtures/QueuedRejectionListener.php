<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Events\ActionOutcomeRejected;
use Illuminate\Contracts\Queue\ShouldQueue;

/**
 * The listener the documentation recommends: queued, so it cannot interrupt the
 * engine. Laravel serialises the whole listener job, event included, which is what
 * puts the event's payload under PHP's `serialize()`.
 */
final class QueuedRejectionListener implements ShouldQueue
{
    public function handle(ActionOutcomeRejected $event): void {}
}
