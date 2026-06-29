<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use Illuminate\Bus\Batch;
use Throwable;

/**
 * The queued rollback continuation, run as a Bus::batch finally-callback once a
 * level's compensations finish. It is a plain invokable object (not a Closure), so
 * the batch repository can serialize it; it simply hands the carried state back to
 * SagaRunner::advance() to evaluate the Stop policy and dispatch the next level or
 * finalize the run. The primary cause is carried as a normalized array (never a
 * Throwable, whose stack trace may hold unserializable closures).
 */
final readonly class AdvanceCompensation
{
    /**
     * @param  list<list<CompensationEntry>>  $remainingLevels
     * @param  array<string, mixed>|null  $primary
     * @param  list<string>  $ranCompensationIds
     */
    public function __construct(
        public string $flowRunId,
        public array $remainingLevels,
        public ?array $primary,
        public FlowStatus $finalState,
        public array $ranCompensationIds,
    ) {}

    /**
     * @throws Throwable
     */
    public function __invoke(Batch $batch): void
    {
        app(SagaRunner::class)->advance(
            $this->flowRunId,
            $this->remainingLevels,
            $this->primary,
            $this->finalState,
            $this->ranCompensationIds,
        );
    }
}
