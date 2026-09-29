<?php

namespace DiscoveryUkraine\SagaLaraFlow\Support;

use DateTimeInterface;

/**
 * Resolved view of the attributes declared on a workflow class. Null fields mean
 * "not declared"; the methods resolve them the same way for a root run and a child.
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

    /**
     * @param  ?string  $surrounding  the parent's for a child, null for a root
     */
    public function connectionWithin(?string $surrounding): ?string
    {
        return $this->connection ?? $surrounding ?? config('saga-lara-flow.queue.connection');
    }

    /**
     * @param  ?string  $surrounding  the parent's for a child, null for a root
     */
    public function queueWithin(?string $surrounding): ?string
    {
        return $this->queue ?? $surrounding ?? config('saga-lara-flow.queue.queue');
    }

    public function expiresAt(): ?DateTimeInterface
    {
        $seconds = $this->timeoutSeconds ?? config('saga-lara-flow.monitor.expiration.defaults.run');

        return $seconds === null ? null : now()->addSeconds((int) $seconds);
    }

    /**
     * @param  array<array-key, string|int|null>  $explicit
     * @return array<int, array{key: string, value: ?string}>
     */
    public function tagsWith(array $explicit): array
    {
        $merged = [];

        foreach ($this->tags as $tag) {
            $merged[$tag['key']] = $tag['value'];
        }

        foreach ($explicit as $key => $value) {
            $merged[(string) $key] = $value === null ? null : (string) $value;
        }

        $normalized = [];

        foreach ($merged as $key => $value) {
            $normalized[] = ['key' => (string) $key, 'value' => $value];
        }

        return $normalized;
    }
}
