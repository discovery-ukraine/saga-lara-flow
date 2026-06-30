<?php

namespace DiscoveryUkraine\SagaLaraFlow;

use DiscoveryUkraine\SagaLaraFlow\Builders\CreateWorkflowBuilder;
use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Queries\FlowQuery;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
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
}
