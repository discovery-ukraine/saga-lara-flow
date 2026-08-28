<?php

namespace DiscoveryUkraine\SagaLaraFlow\Concerns\Workflow;

use DiscoveryUkraine\SagaLaraFlow\Exceptions\RetryPolicyReentryException;

/**
 * Read-only access to the current run's identity/metadata, plus queryable tags,
 * attachable one at a time or in bulk.
 */
trait ProvidesFlowMetadata
{
    /**
     * Attach a queryable tag to the current run. Idempotent across replays.
     *
     * @throws RetryPolicyReentryException
     */
    public function tag(string $key, string|int|null $value = null): void
    {
        // The only workflow-facing write that never takes an ordinal, so
        // nextSequence() cannot cover it: guard it where it happens.
        if ($this->runtime->isDecidingRun($this->runtime->run()->id)) {
            throw RetryPolicyReentryException::for('tag()');
        }

        $this->runtime->run()->tags()->updateOrCreate(
            ['key' => $key],
            ['value' => $value === null ? null : (string) $value],
        );
    }

    /**
     * Attach several queryable tags at once, keyed by tag name. A null value
     * records a tag with no value. Idempotent across replays.
     *
     * @param  array<string, string|int|null>  $tags
     */
    public function tags(array $tags): void
    {
        foreach ($tags as $key => $value) {
            $this->tag($key, $value);
        }
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
