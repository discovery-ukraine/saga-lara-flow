<?php

namespace DiscoveryUkraine\SagaLaraFlow\Jobs;

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Data\CompensationDefinition;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationStatus;
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
 */
class RunCompensationJob implements ShouldQueue
{
    use Batchable;
    use Queueable;

    public function __construct(
        public string $flowRunId,
        public string $compensationRunId,
        public CompensationDefinition $definition,
    ) {}

    public function handle(CompensationExecutor $executor, FlowRepository $repository, TenancyManager $tenancy): void
    {
        if ($this->batch()?->cancelled()) {
            return;
        }

        $flowRun = $repository->find($this->flowRunId);

        if ($flowRun === null) {
            return;
        }

        $compensation = $this->resolveCompensation();

        if ($compensation === null) {
            return;
        }

        if (in_array($compensation->status, [CompensationStatus::Completed, CompensationStatus::Failed], true)) {
            return;
        }

        $tenancy->for(
            $flowRun,
            $flowRun->workflow_class,
            fn () => $executor->execute($compensation, $this->definition),
        );
    }

    private function resolveCompensation(): ?CompensationRun
    {
        /** @var class-string<CompensationRun> $model */
        $model = config('saga-lara-flow.models.compensation_run');

        return $model::query()->find($this->compensationRunId);
    }
}
