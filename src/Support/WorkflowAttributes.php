<?php

namespace DiscoveryUkraine\SagaLaraFlow\Support;

/**
 * Resolved view of the attributes declared on a workflow class. Null fields mean
 * "not declared" so callers can apply the explicit > attribute > config chain.
 */
final readonly class WorkflowAttributes
{
    /**
     * @param  array<int, array{key: string, value: ?string}>  $tags
     */
    public function __construct(
        public ?string $name = null,
        public ?string $version = null,
        public ?string $connection = null,
        public ?string $queue = null,
        public ?int $timeoutSeconds = null,
        public array $tags = [],
    ) {}
}
