<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use Illuminate\Bus\Batch;
use Throwable;

/**
 * An invokable object, not a Closure, so the batch repository can serialize it. $rollbackToken is
 * declared, not promoted: an older continuation is unserialized without the constructor.
 */
final class AdvanceCompensation
{
    public ?string $rollbackToken = null;

    /**
     * @param  list<list<CompensationEntry>>  $remainingLevels
     * @param  array<string, mixed>|null  $primary
     * @param  list<string>  $ranCompensationIds
     */
    public function __construct(
        public readonly string $flowRunId,
        public readonly array $remainingLevels,
        public readonly ?array $primary,
        public readonly FlowStatus $finalState,
        public readonly array $ranCompensationIds,
        ?string $rollbackToken = null,
    ) {
        $this->rollbackToken = $rollbackToken;
    }

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
            $this->rollbackToken,
        );
    }
}
