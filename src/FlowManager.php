<?php

namespace DiscoveryUkraine\SagaLaraFlow;

use DiscoveryUkraine\SagaLaraFlow\Builders\CreateWorkflowBuilder;
use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Database\Eloquent\Builder;

readonly class FlowManager
{
    public function __construct(
        private FlowRepository $repository,
    ) {}

    public function create(string $workflowClass): CreateWorkflowBuilder
    {
        return new CreateWorkflowBuilder($workflowClass, $this->repository);
    }

    public function findRun(string $id): ?FlowRun
    {
        return $this->repository->find($id);
    }

    public function run(string $id): FlowHandle
    {
        return new FlowHandle($this->repository->findOrFail($id));
    }

    public function query(): Builder
    {
        return config('saga-lara-flow.models.flow_run')::query();
    }
}
