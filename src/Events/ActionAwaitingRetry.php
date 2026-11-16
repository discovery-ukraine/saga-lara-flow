<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use Illuminate\Contracts\Events\ShouldDispatchAfterCommit;

/**
 * Dispatched when a failed step parks on its retryOnSignal() signal instead of
 * failing the flow. Unlike ActionFailed this is not terminal: the step keeps its
 * ordinal, its exception and its place in the saga while it waits for $signal (or
 * for the wait to time out). Exactly one fires per parking.
 *
 * Dispatched after the surrounding transaction commits, so a listener never reacts
 * to a parking the database rolled back.
 */
final readonly class ActionAwaitingRetry implements ShouldDispatchAfterCommit
{
    public function __construct(
        public ActionRun $actionRun,
        public string $signal,
    ) {}
}
