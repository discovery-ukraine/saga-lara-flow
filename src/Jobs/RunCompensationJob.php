<?php

namespace DiscoveryUkraine\SagaLaraFlow\Jobs;

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Data\CompensationDefinition;
use DiscoveryUkraine\SagaLaraFlow\Middleware\LockMiddlewareFactory;
use DiscoveryUkraine\SagaLaraFlow\Models\CompensationRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\CompensationExecutor;
use DiscoveryUkraine\SagaLaraFlow\Support\TenancyManager;
use Illuminate\Bus\Batchable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;

/**
 * $rollbackToken is declared, not promoted: an older payload is unserialized without the
 * constructor and claims, as null, for whoever owns the rollback.
 */
class RunCompensationJob implements ShouldQueue
{
    use Batchable;
    use Queueable;

    public ?string $rollbackToken = null;

    public function __construct(
        public string $flowRunId,
        public string $compensationRunId,
        public CompensationDefinition $definition,
        ?string $rollbackToken = null,
    ) {
        $this->rollbackToken = $rollbackToken;
    }

    public function handle(CompensationExecutor $executor, FlowRepository $repository, TenancyManager $tenancy): void
    {
        if ($this->batch()?->cancelled()) {
            return;
        }

        $flowRun = $repository->findOnWriter($this->flowRunId);

        if ($flowRun === null) {
            return;
        }

        $compensation = $this->resolveCompensation();

        if ($compensation === null) {
            return;
        }

        // A lost claim is quiet: the batch's finally callback settles the level.
        $tenancy->for(
            $flowRun,
            $flowRun->workflow_class,
            fn () => $executor->execute($compensation, $this->definition, $this->rollbackToken),
        );
    }

    /**
     * @return array<int, object>
     */
    public function middleware(): array
    {
        return app(LockMiddlewareFactory::class)->compensationMiddleware($this->compensationRunId);
    }

    private function resolveCompensation(): ?CompensationRun
    {
        /** @var class-string<CompensationRun> $model */
        $model = config('saga-lara-flow.models.compensation_run');

        return $model::query()->useWritePdo()->find($this->compensationRunId);
    }
}
