<?php

namespace DiscoveryUkraine\SagaLaraFlow\Attributes;

use Attribute;

/**
 * Declarative per-step wall-clock deadline (in seconds from schedule), mapped to
 * the action's expires_at. An explicit ->expiresAt() on the action builder wins
 * (precedence: explicit call > attribute > config).
 */
#[Attribute(Attribute::TARGET_CLASS)]
final class ActionTimeout
{
    public function __construct(
        public int $seconds,
    ) {}
}
