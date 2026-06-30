<?php

namespace DiscoveryUkraine\SagaLaraFlow\Attributes;

use Attribute;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;

/**
 * Declarative default close policy applied when this workflow is awaited as a
 * child. An explicit ->closePolicy() on the child builder wins; the configured
 * default is the final fallback (precedence: explicit call > attribute > config).
 */
#[Attribute(Attribute::TARGET_CLASS)]
final class ChildPolicy
{
    public function __construct(
        public ChildClosePolicy $policy,
    ) {}
}
