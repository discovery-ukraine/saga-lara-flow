<?php

namespace DiscoveryUkraine\SagaLaraFlow\Attributes;

use Attribute;

/**
 * Declarative display name for an action, stored on its action_run row (used by
 * history and the CLI inspector). Falls back to the class basename when absent.
 */
#[Attribute(Attribute::TARGET_CLASS)]
final class ActionName
{
    public function __construct(
        public string $name,
    ) {}
}
