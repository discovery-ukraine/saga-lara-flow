<?php

namespace DiscoveryUkraine\SagaLaraFlow\Repositories;

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowNotFoundException;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

class EloquentFlowRepository implements FlowRepository
{
    public function create(array $attributes, array $tags = []): FlowRun
    {
        $model = $this->flowRunClass();

        $run = new $model;
        $run->fill($attributes);
        $run->save();

        foreach ($tags as $tag) {
            $run->tags()->create([
                'key' => $tag['key'],
                'value' => $tag['value'] ?? null,
            ]);
        }

        return $run;
    }

    public function find(string $id): ?FlowRun
    {
        return $this->flowRunClass()::query()->find($id);
    }

    public function findOrFail(string $id): FlowRun
    {
        return $this->find($id) ?? throw FlowNotFoundException::for($id);
    }

    /**
     * @return class-string<FlowRun>
     */
    private function flowRunClass(): string
    {
        return config('saga-lara-flow.models.flow_run');
    }
}
