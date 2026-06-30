<?php

namespace DiscoveryUkraine\SagaLaraFlow\Support;

/**
 * Resolved view of the attributes declared on an action class. Null fields mean
 * "not declared" so the action builder can apply the explicit > attribute chain.
 */
final readonly class ActionAttributes
{
    public function __construct(
        public ?string $name = null,
        public ?int $timeoutSeconds = null,
        public ?bool $continueOnFailure = null,
    ) {}
}
