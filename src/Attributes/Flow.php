<?php

namespace DiscoveryUkraine\SagaLaraFlow\Attributes;

use Attribute;

/**
 * Declarative workflow identity: a stable name and an optional version, read at
 * create time. An explicit ->version() on the builder still wins (§20 precedence:
 * explicit call > attribute > config).
 */
#[Attribute(Attribute::TARGET_CLASS)]
final class Flow
{
    public function __construct(
        public ?string $name = null,
        public ?string $version = null,
    ) {}
}
