<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Events\ActionOutcomeRejected;
use RuntimeException;

/**
 * A synchronous listener that throws — the contract the documentation asks hosts not
 * to break, and which on this one path must not be allowed to fail the job.
 */
final class ThrowingRejectionListener
{
    public function handle(ActionOutcomeRejected $event): void
    {
        throw new RuntimeException('listener blew up');
    }
}
