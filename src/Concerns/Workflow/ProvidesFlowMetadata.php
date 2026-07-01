<?php

namespace DiscoveryUkraine\SagaLaraFlow\Concerns\Workflow;

/**
 * Read-only access to the current run's identity/metadata, plus queryable tags.
 */
trait ProvidesFlowMetadata
{
    /**
     * Attach a queryable tag to the current run. Idempotent across replays.
     */
    public function tag(string $key, string|int|null $value = null): void
    {
        $this->runtime->run()->tags()->updateOrCreate(
            ['key' => $key],
            ['value' => $value === null ? null : (string) $value],
        );
    }

    public function runId(): string
    {
        return $this->runtime->run()->id;
    }

    public function flowName(): ?string
    {
        return $this->runtime->run()->workflow_name;
    }

    public function version(): ?string
    {
        return $this->runtime->run()->workflow_version;
    }

    public function parentRunId(): ?string
    {
        return $this->runtime->run()->parent_id;
    }
}
