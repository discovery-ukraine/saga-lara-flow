<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Action;
use DiscoveryUkraine\SagaLaraFlow\Attributes\ContinueOnFailure;
use RuntimeException;

/**
 * Always fails, but is marked optional via #[ContinueOnFailure]: the attribute
 * alone (no ->continueOnFailure() call) must make its failure non-fatal.
 */
#[ContinueOnFailure]
final class OptionalAttributedAction extends Action
{
    public function handle(): mixed
    {
        throw new RuntimeException('boom');
    }
}
