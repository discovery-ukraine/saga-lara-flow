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
 * Executes a single compensation inside a Bus::batch level. It carries the
 * CompensationDefinition verbatim (closures via SerializableClosure), so it can
 * run in a separate worker/process. The job never throws: the compensation
 * outcome is recorded on the CompensationRun, and SagaRunner reads that status
 * after the batch to apply the Stop/Continue policy.
 *
 * $rollbackToken is the owner of the rollback that sent it, so a job of an owner the
 * rollback was taken from claims nothing. It is a plain declared property rather than a
 * promoted one: a payload queued before it existed is unserialized without the
 * constructor, and only a class-level default keeps it initialized — as null, which
 * claims for whoever owns the rollback.
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

        // No status pre-check: the claim inside execute() is the single source of
        // truth — a row already settled, or still Running within its reclaim window,
        // fails the claim and nothing runs. A lost claim is quiet here because the
        // batch's own ->finally(new AdvanceCompensation(...)) settles the level.

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
