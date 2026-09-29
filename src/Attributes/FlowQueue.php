<?php

namespace DiscoveryUkraine\SagaLaraFlow\Attributes;

use Attribute;

/**
 * Declarative queue transport for a workflow's jobs, read at create time, each
 * field on its own. An explicit ->onConnection()/->onQueue() on the builder wins;
 * below the attribute stand the parent's for a child run, then config.
 */
#[Attribute(Attribute::TARGET_CLASS)]
final class FlowQueue
{
    public function __construct(
        public ?string $connection = null,
        public ?string $queue = null,
    ) {}
}
