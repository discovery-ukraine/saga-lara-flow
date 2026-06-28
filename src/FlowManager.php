<?php

namespace DiscoveryUkraine\SagaLaraFlow;

use DiscoveryUkraine\SagaLaraFlow\Builders\CreateWorkflowBuilder;
use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use Illuminate\Database\Eloquent\Builder;

readonly class FlowManager
{
    public function __construct(
        private FlowRepository $repository,
        private FlowExecutor $executor,
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

    public function query(): Builder
    {
        return config('saga-lara-flow.models.flow_run')::query();
    }
}
