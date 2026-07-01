<?php

namespace DiscoveryUkraine\SagaLaraFlow;

use DiscoveryUkraine\SagaLaraFlow\Builders\CreateWorkflowBuilder;
use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Queries\FlowQuery;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Support\TenancyManager;
use Illuminate\Support\Traits\Macroable;

class FlowManager
{
    use Macroable;

    public function __construct(
        private readonly FlowRepository $repository,
        private readonly FlowExecutor $executor,
        private readonly FlowDoctor $doctor,
    ) {}

    public function create(string $workflowClass): CreateWorkflowBuilder
    {
        return new CreateWorkflowBuilder($workflowClass, $this->repository, $this->executor);
    }

    public function findRun(string $id): ?FlowRun
    {
        return $this->repository->find($id);
    }

    public function loadFlow(string $id): FlowHandle
    {
        return new FlowHandle($this->repository->findOrFail($id));
    }

    public function query(): FlowQuery
    {
        /** @var class-string<FlowRun> $model */
        $model = config('saga-lara-flow.models.flow_run');

        return new FlowQuery($model::query());
    }

    /**
     * Manually re-drive a stuck run (the doctor's escape hatch). Re-dispatches the
     * workflow so replay resumes it; a terminal run is left untouched. Throws when
     * the run does not exist.
     */
    public function kick(string $id): FlowRun
    {
        return $this->doctor->kick($this->repository->findOrFail($id));
    }

    /**
     * The tenant context of the run currently executing, for host code inside a
     * workflow/action handle() that manages tenancy itself. Null outside a driven
     * run, or when no tenant was captured at creation.
     *
     * @return array<int|string, mixed>|null
     */
    public function tenancyContext(): ?array
    {
        return app(TenancyManager::class)->context();
    }
}
