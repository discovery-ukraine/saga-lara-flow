<?php

namespace DiscoveryUkraine\SagaLaraFlow\Builders;

use DateTimeInterface;
use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\WorkflowClassMissingException;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

class CreateWorkflowBuilder
{
    /** @var array<int, mixed> */
    private array $arguments = [];

    private ?string $connection = null;

    private ?string $queue = null;

    /** @var array<string, ?string> */
    private array $tags = [];

    private ?string $version = null;

    private ?DateTimeInterface $expiresAt = null;

    public function __construct(
        private readonly string $workflowClass,
        private readonly FlowRepository $repository,
    ) {
        if (! class_exists($this->workflowClass)) {
            throw WorkflowClassMissingException::for($this->workflowClass);
        }
    }

    public function withArguments(mixed ...$arguments): static
    {
        $this->arguments = array_values($arguments);

        return $this;
    }

    public function onConnection(?string $connection): static
    {
        $this->connection = $connection;

        return $this;
    }

    public function onQueue(?string $queue): static
    {
        $this->queue = $queue;

        return $this;
    }

    /**
     * @param  array<string, ?string>  $tags
     */
    public function withTags(array $tags): static
    {
        $this->tags = array_merge($this->tags, $tags);

        return $this;
    }

    public function version(?string $version): static
    {
        $this->version = $version;

        return $this;
    }

    public function expiresAt(?DateTimeInterface $expiresAt): static
    {
        $this->expiresAt = $expiresAt;

        return $this;
    }

    /**
     * Dispatch the workflow onto the queue.
     *
     * Phase 1 persists the run; queued execution is wired in a later phase.
     */
    public function run(): FlowRun
    {
        return $this->persist();
    }

    /**
     * Execute the workflow synchronously.
     *
     * Phase 1 persists the run; inline execution is wired in a later phase.
     */
    public function runSync(): FlowRun
    {
        return $this->persist();
    }

    private function persist(): FlowRun
    {
        return $this->repository->create([
            'workflow_class' => $this->workflowClass,
            'workflow_version' => $this->version,
            'status' => FlowStatus::Pending,
            'arguments' => $this->arguments,
            'connection' => $this->connection ?? config('saga-lara-flow.queue.connection'),
            'queue' => $this->queue ?? config('saga-lara-flow.queue.queue'),
            'expires_at' => $this->expiresAt,
        ], $this->normalizedTags());
    }

    /**
     * @return array<int, array{key: string, value: ?string}>
     */
    private function normalizedTags(): array
    {
        $normalized = [];

        foreach ($this->tags as $key => $value) {
            $normalized[] = [
                'key' => (string) $key,
                'value' => $value === null ? null : (string) $value,
            ];
        }

        return $normalized;
    }
}
