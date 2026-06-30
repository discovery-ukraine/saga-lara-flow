<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;

/**
 * Dispatched when an optional action (continueOnFailure) exhausts its retries and
 * is recorded as OptionalFailed. Unlike ActionFailed this never fails the flow —
 * it surfaces a best-effort step that gave up so listeners can observe it. The
 * action's stored exception remains on the row.
 */
final readonly class OptionalActionFailed
{
    public function __construct(
        public ActionRun $actionRun,
    ) {}
}
