<?php

namespace DiscoveryUkraine\SagaLaraFlow\Attributes;

use Attribute;

/**
 * Declarative "best-effort step": marks an action optional so its failure does
 * not fail the flow (it lands OptionalFailed and run() returns the fallback). An
 * explicit ->continueOnFailure() on the action builder wins.
 */
#[Attribute(Attribute::TARGET_CLASS)]
final class ContinueOnFailure
{
    public function __construct(
        public bool $continue = true,
    ) {}
}
