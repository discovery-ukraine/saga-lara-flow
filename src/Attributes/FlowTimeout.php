<?php

namespace DiscoveryUkraine\SagaLaraFlow\Attributes;

use Attribute;

/**
 * Declarative run-level wall-clock deadline (in seconds from create), mapped to
 * the run's expires_at. An explicit ->expiresAt() wins; the monitor's configured
 * default is the final fallback (precedence: explicit call > attribute > config).
 */
#[Attribute(Attribute::TARGET_CLASS)]
final class FlowTimeout
{
    public function __construct(
        public int $seconds,
    ) {}
}
