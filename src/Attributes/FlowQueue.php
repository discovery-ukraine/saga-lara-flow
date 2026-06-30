<?php

namespace DiscoveryUkraine\SagaLaraFlow\Attributes;

use Attribute;

/**
 * Declarative queue transport for a workflow's jobs, read at create time. An
 * explicit ->onConnection()/->onQueue() on the builder wins; config is the
 * final fallback (precedence: explicit call > attribute > config).
 */
#[Attribute(Attribute::TARGET_CLASS)]
final class FlowQueue
{
    public function __construct(
        public ?string $connection = null,
        public ?string $queue = null,
    ) {}
}
