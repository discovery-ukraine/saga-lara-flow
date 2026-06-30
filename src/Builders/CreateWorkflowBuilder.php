<?php

namespace DiscoveryUkraine\SagaLaraFlow\Builders;

use DateTimeInterface;
use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\WorkflowClassMissingException;
use DiscoveryUkraine\SagaLaraFlow\Jobs\RunWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Support\AttributeReader;
use DiscoveryUkraine\SagaLaraFlow\Support\WorkflowAttributes;
use Throwable;

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
        private readonly FlowExecutor $executor,
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
     * Persist the run and dispatch it onto the queue. Returns the run as it
     * stands after dispatch (Pending on a real queue; further along when the
     * sync queue driver runs it inline).
     */
    public function run(): FlowRun
    {
        $run = $this->persist();

        $job = RunWorkflowJob::dispatch($run->id);

        if ($run->connection !== null) {
            $job->onConnection($run->connection);
        }

        if ($run->queue !== null) {
            $job->onQueue($run->queue);
        }

        if (config('saga-lara-flow.queue.after_commit')) {
            $job->afterCommit();
        }

        return $run->refresh();
    }

    /**
     * Persist the run and execute it synchronously via the drive loop. Returns
     * the run in its resulting state (Completed/Failed/Waiting).
     *
     * @throws Throwable
     */
    public function runSync(): FlowRun
    {
        return $this->executor->drive($this->persist(), RunMode::Sync);
    }

    private function persist(): FlowRun
    {
        $attributes = app(AttributeReader::class)->workflow($this->workflowClass);

        return $this->repository->create([
            'workflow_class' => $this->workflowClass,
            'workflow_name' => $attributes->name,
            'workflow_version' => $this->version ?? $attributes->version,
            'status' => FlowStatus::Pending,
            'arguments' => $this->arguments,
            'connection' => $this->connection ?? $attributes->connection ?? config('saga-lara-flow.queue.connection'),
            'queue' => $this->queue ?? $attributes->queue ?? config('saga-lara-flow.queue.queue'),
            'expires_at' => $this->expiresAt ?? $this->attributeExpiry($attributes) ?? $this->defaultExpiry(),
        ], $this->normalizedTags($attributes));
    }

    private function attributeExpiry(WorkflowAttributes $attributes): ?DateTimeInterface
    {
        return $attributes->timeoutSeconds === null
            ? null
            : now()->addSeconds($attributes->timeoutSeconds);
    }

    private function defaultExpiry(): ?DateTimeInterface
    {
        $seconds = config('saga-lara-flow.monitor.expiration.defaults.run');

        return $seconds === null ? null : now()->addSeconds((int) $seconds);
    }

    /**
     * Merge attribute-declared tags with the builder's explicit tags. An explicit
     * tag with the same key overrides the attribute's value (precedence).
     *
     * @return array<int, array{key: string, value: ?string}>
     */
    private function normalizedTags(WorkflowAttributes $attributes): array
    {
        $merged = [];

        foreach ($attributes->tags as $tag) {
            $merged[$tag['key']] = $tag['value'];
        }

        foreach ($this->tags as $key => $value) {
            $merged[(string) $key] = $value === null ? null : (string) $value;
        }

        $normalized = [];

        foreach ($merged as $key => $value) {
            $normalized[] = ['key' => (string) $key, 'value' => $value];
        }

        return $normalized;
    }
}
